"""Loopback webhook receiver, live monitoring checks and isolated resilience drills."""
import argparse
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import subprocess
import time
from urllib.request import urlopen
import uuid


def assert_isolated_container(info):
    labels = info.get('Config', {}).get('Labels') or {}
    project = labels.get('com.docker.compose.project', '')
    if not re.fullmatch(r'sentinel-resilience-[a-z0-9-]+', project) or labels.get('sentinel.drill') != 'true':
        raise ValueError('Refusing to affect a non-owned resilience container')


def webhook_server(path, host='127.0.0.1', port=18093):
    path = Path(path)
    if path.is_symlink():
        raise ValueError('Webhook output cannot be a symlink')
    path.parent.mkdir(parents=True, exist_ok=True)
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path != '/alerts':
                self.send_error(404); return
            try:
                size = int(self.headers.get('Content-Length', '0'))
                if not 0 < size <= 1024 * 1024:
                    raise ValueError('Invalid webhook size')
                payload = json.loads(self.rfile.read(size))
                if not isinstance(payload.get('alerts'), list) or payload.get('status') not in ('firing', 'resolved'):
                    raise ValueError('Invalid alert notification')
                record = {'received_at': datetime.now(timezone.utc).isoformat(), 'notification': payload}
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
                with os.fdopen(fd, 'a') as stream:
                    stream.write(json.dumps(record, ensure_ascii=False)+'\n')
                    stream.flush(); os.fsync(stream.fileno())
            except (ValueError, OSError):
                self.send_error(400); return
            self.send_response(200); self.end_headers()
        def log_message(self, *_):
            pass
    return ThreadingHTTPServer((host, port), Handler)


def verify_stack():
    checks = {}
    for name, url in [('prometheus', 'http://127.0.0.1:9090/-/ready'),
                      ('alertmanager', 'http://127.0.0.1:9093/-/ready'),
                      ('grafana', 'http://127.0.0.1:3000/api/health')]:
        with urlopen(url, timeout=5) as response:
            checks[name] = response.status == 200
    with urlopen('http://127.0.0.1:9090/api/v1/targets', timeout=5) as response:
        targets = json.load(response)['data']['activeTargets']
    jobs = {target['labels']['job']: target['health'] for target in targets}
    for job in ('sentinel-platform', 'sentinel-dependencies', 'sentinel-operations'):
        if jobs.get(job) != 'up':
            raise RuntimeError('Metrics target is not healthy: '+job)
    with urlopen('http://127.0.0.1:9090/api/v1/rules', timeout=5) as response:
        rules = [rule['name'] for group in json.load(response)['data']['groups'] for rule in group['rules']]
    if 'SentinelPipelineFailed' not in rules:
        raise RuntimeError('Operational alert rules not loaded')
    return {'services': checks, 'targets': jobs, 'loaded_rules': rules,
            'boundary': 'HTTP services, real scrapes and loaded rules; notification delivery needs receiver evidence'}


def _container(project, service):
    if not re.fullmatch(r'sentinel-resilience-[a-z0-9-]+', project):
        raise ValueError('Project must have sentinel-resilience- prefix')
    ids = subprocess.check_output(['docker', 'ps', '--all', '--filter', 'label=com.docker.compose.project='+project,
                                  '--filter', 'label=com.docker.compose.service='+service, '--format', '{{.ID}}'], text=True).split()
    if len(ids) != 1:
        raise ValueError('Expected exactly one isolated service container')
    info = json.loads(subprocess.check_output(['docker', 'inspect', ids[0]], text=True))[0]
    assert_isolated_container(info)
    if not info['State']['Running']:
        raise ValueError('Drill requires a running container')
    return ids[0]


def kafka_drill(project, service, rows=100):
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    container = _container(project, service)
    # Bind endpoints to inspected containers, rather than trusting fixed ports
    # that might belong to a different deployment.
    for index, port in enumerate((39094, 39095, 39096), start=1):
        broker = _container(project, 'kafka'+str(index))
        info = json.loads(subprocess.check_output(['docker', 'inspect', broker], text=True))[0]
        bindings = info.get('NetworkSettings', {}).get('Ports', {}).get(str(port)+'/tcp') or []
        if not any(item.get('HostIp') == '127.0.0.1' and item.get('HostPort') == str(port) for item in bindings):
            raise ValueError('Kafka drill endpoint does not belong to the inspected isolated broker')
    servers = ['127.0.0.1:39094', '127.0.0.1:39095', '127.0.0.1:39096']
    topic = 'sentinel-chaos-'+uuid.uuid4().hex
    admin = KafkaAdminClient(bootstrap_servers=servers)
    admin.create_topics([NewTopic(topic, 3, 3, topic_configs={'min.insync.replicas': '2'})])
    producer = KafkaProducer(bootstrap_servers=servers, acks='all', retries=10,
                             request_timeout_ms=30000, max_block_ms=60000)
    expected = [json.dumps({'id': n, 'source_group': 'simulation', 'acquisition_method': 'generated'}, sort_keys=True).encode() for n in range(rows)]
    started = time.monotonic()
    acknowledged = 0
    try:
        # Baseline and degraded phases both require acknowledged writes.
        cut = max(1, rows // 2)
        for value in expected[:cut]:
            producer.send(topic, value=value).get(timeout=60); acknowledged += 1
        subprocess.run(['docker', 'stop', '--time', '10', container], check=True, capture_output=True)
        for value in expected[cut:]:
            producer.send(topic, value=value).get(timeout=60); acknowledged += 1
    finally:
        producer.close()
        subprocess.run(['docker', 'start', container], check=True, capture_output=True)
    consumer = KafkaConsumer(topic, bootstrap_servers=servers, auto_offset_reset='earliest',
                             enable_auto_commit=False, group_id='drill-'+uuid.uuid4().hex)
    values = []
    deadline = time.monotonic()+90
    try:
        while len(values) < len(expected) and time.monotonic() < deadline:
            for batch in consumer.poll(timeout_ms=1000).values():
                values.extend(row.value for row in batch)
    finally:
        consumer.close()
    digest = lambda items: hashlib.sha256(b'\n'.join(sorted(set(items)))).hexdigest()
    if set(values) != set(expected):
        raise RuntimeError('Acknowledged Kafka payload reconciliation failed; topic retained: '+topic)
    result = {'topic': topic, 'acknowledged': acknowledged, 'received': len(values),
              'unique': len(set(values)), 'business_sha256': digest(values), 'seconds': time.monotonic()-started,
              'boundary': 'one broker process outage, acks=all, actual Kafka payload reconciliation; not whole-host HA'}
    admin.delete_topics([topic]); admin.close()
    return result


def spark_workload(path, rows):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('sentinel-resilience-drill').getOrCreate()
    started = time.monotonic()
    try:
        expected = hashlib.sha256('\n'.join(str(n) for n in range(rows)).encode()).hexdigest()
        digests = []
        for _ in range(5):
            data = spark.range(rows, numPartitions=32).repartition(32).orderBy('id').collect()
            actual = hashlib.sha256('\n'.join(str(row.id) for row in data).encode()).hexdigest()
            if actual != expected:
                raise RuntimeError('Spark task recovery changed output IDs')
            digests.append(actual)
        result = {'rows': rows, 'iterations': len(digests), 'business_sha256': expected, 'seconds': time.monotonic()-started}
        Path(path).write_text(json.dumps(result)+'\n')
        return result
    finally:
        spark.stop()


def spark_drill(project, service, rows):
    worker = _container(project, service)
    master = _container(project, 'spark-master')
    result_path = '/tmp/sentinel-drill/result-'+uuid.uuid4().hex+'.json'
    command = ['docker', 'exec', master, '/opt/spark/bin/spark-submit', '--master', 'spark://spark-master:7077',
               '--conf', 'spark.driver.host=spark-master', '--conf', 'spark.driver.bindAddress=0.0.0.0',
               '--conf', 'spark.cores.max=4', '--driver-memory', '1g', '--executor-memory', '1g',
               '/opt/sentinel/verify_operations_stack.py', '--action', 'spark-workload', '--output', result_path, '--rows', str(rows)]
    with subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) as process:
        try:
            time.sleep(5)
            if process.poll() is not None:
                raise RuntimeError('Spark workload ended before the fault could be injected')
            subprocess.run(['docker', 'stop', '--time', '10', worker], check=True, capture_output=True)
            process.wait(timeout=300)
            if process.returncode:
                raise RuntimeError('Spark workload failed during worker outage')
        finally:
            subprocess.run(['docker', 'start', worker], check=True, capture_output=True)
            if process.poll() is None:
                process.terminate(); process.wait(timeout=10)
    result = json.loads(subprocess.check_output(['docker', 'exec', master, 'cat', result_path], text=True))
    result['boundary'] = 'worker container outage, deterministic shuffled ID reconciliation; not multi-host HA or SQL/UI latency'
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--action', choices=['receiver', 'verify', 'chaos', 'spark-workload'], required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--project', default='sentinel-resilience-drill')
    parser.add_argument('--service', choices=['kafka1','kafka2','kafka3','spark-worker1','spark-worker2'], default='kafka1')
    parser.add_argument('--rows', type=int, default=100000)
    args = parser.parse_args()
    if not 2 <= args.rows <= 1000000:
        parser.error('Rows must be between2 and1000000')
    if args.action == 'receiver':
        with webhook_server(args.output) as server:
            server.serve_forever()
        return
    if args.output.exists():
        parser.error('Output exists; preserve earlier evidence')
    if args.action == 'verify':
        result = verify_stack()
    elif args.action == 'spark-workload':
        spark_workload(args.output, args.rows); return
    elif args.service.startswith('kafka'):
        result = kafka_drill(args.project, args.service, min(args.rows,10000))
    else:
        result = spark_drill(args.project, args.service, args.rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2); stream.write('\n')


if __name__ == '__main__':
    main()
