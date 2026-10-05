#!/usr/bin/env bash
set -e

WSL_IP=$(hostname -I | awk '{print $1}')

cat > /opt/kafka/config/sentinel-kraft.properties <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://${WSL_IP}:9092,EXTERNAL://${WSL_IP}:9094
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT
num.network.threads=3
num.io.threads=4
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/var/lib/kafka-logs
num.partitions=3
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
EOF

if ! ss -tln | grep -q ":9092 "; then
    rm -rf /var/lib/kafka-logs
    mkdir -p /var/lib/kafka-logs
    CLUSTER_ID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
    /opt/kafka/bin/kafka-storage.sh format -t "$CLUSTER_ID" -c /opt/kafka/config/sentinel-kraft.properties
    export KAFKA_HEAP_OPTS="-Xmx512M -Xms256M"
    /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/sentinel-kraft.properties
fi

for i in $(seq 1 15); do
    if ss -tln | grep -q ":9092 "; then
        echo "Kafka KRaft broker is UP on ${WSL_IP}:9092 and ${WSL_IP}:9094"
        exit 0
    fi
    sleep 1
done

cat /opt/kafka/logs/server.log | tail -n 30
exit 1
