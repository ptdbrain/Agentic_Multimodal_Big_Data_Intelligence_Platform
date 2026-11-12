# Native operational deployment

Run from `btl/`. Templates under `orchestration/systemd/` are user units. Install only the selected native or Docker monitoring deployment: listeners cannot be shared. Keep all configuration/environment files outside Git, mode0600. Vendor PostgreSQL16, Kafka, Java17, Prometheus2.54.1, Alertmanager0.27.0 and Grafana11.2.0 must already be installed; these launchers do not download unverified binaries.

## Core services

`~/.config/sentinel/native-services.env` supplies `SENTINEL_PROJECT_DIR`, `SENTINEL_PROJECT_PYTHON` and `SENTINEL_NATIVE_CONFIG`. The JSON config has:

```json
{
  "persistent_root": "/srv/sentinel",
  "postgres": {"binary":"/opt/postgresql/bin/postgres","data_dir":"/srv/sentinel/pg-data","socket_dir":"/srv/sentinel/pg-sockets","port":25432},
  "minio": {"binary":"/opt/minio","data_dir":"/srv/sentinel/minio-data","port":29000,"console_port":29001,"environment_file":"/private/native.env"},
  "kafka": {"binary":"/opt/kafka/bin/kafka-server-start.sh","properties":"/srv/sentinel/server.properties","java_home":"/opt/java17"}
}
```

Initialize vendor data/config once using their own tools. MinIO's environment file supplies `MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY`; Kafka properties declare persistent `log.dirs` and loopback advertised listeners. Never point rehearsal services at serving data. Scheduler deployment adds `Requires`/`After` dependencies on the three core units.

```bash
systemctl --user daemon-reload
systemctl --user enable --now sentinel-native@postgres sentinel-native@minio sentinel-native@kafka
systemctl --user enable --now sentinel-source-scheduler.service
loginctl enable-linger "$USER"
```

Linger starts the user service manager at boot and keeps it after logout. Verify `loginctl show-user "$USER" -p Linger`; an enabled unit alone does not prove boot recovery. Stop previously owned detached processes before adopting their ports; verify PID identity before sending signals.

## Monitoring

`monitoring.env` supplies project/Python paths and `SENTINEL_MONITORING_CONFIG`. The private JSON declares `work` (existing persistent Grafana/provisioning/TSDB directory), `tools` (vendor Prometheus/Alertmanager parent), `python`, `environment_file` (plain quoted KEY=VALUE assignments), `source_jobs` and `backup_status_file`. Provision Grafana using the repository dashboards and its documented datasource before startup. `work/prometheus.yml` uses absolute checked-in alert-rule paths; existing `admin-password` and `secret-key` files remain private. Shell scripts containing `source` or `export` are not assignment files; flatten their trusted deployment values first.

Enable `sentinel-monitoring@metrics`, `@dependencies`, `@operations`, `@receiver`, `@prometheus`, `@alertmanager` and `@grafana`. Ports8000–8002,18093,9090,9093,3000 bind loopback. Verify `/api/health` on Grafana, `/-/healthy` on Prometheus/Alertmanager, and all three Prometheus `up` targets. `systemctl --user status UNIT` and `journalctl --user -u UNIT` are the lifecycle/log interface. Do not print private environments or Spark command lines containing credentials.

## Maintenance schedules

Enable the managed backup02:00, source retention03:00 and hourly source acceptance timers only after their first explicit execution succeeds. Install their matching private environment files as described in [backup](operations-backup.md), [Kafka](operations-kafka.md) and [source jobs](managed-source-jobs.md). Inspect with `systemctl --user list-timers 'sentinel-*'`. Keep the old Tiki capture and old generic backup timers disabled to prevent duplicate ownership.
