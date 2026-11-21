FROM prom/prometheus:v2.54.1
COPY monitoring/prometheus-application.yml /etc/prometheus/prometheus.yml
COPY monitoring/alerts.yml /etc/prometheus/alerts.yml
COPY monitoring/datasets-alerts.yml /etc/prometheus/datasets-alerts.yml
RUN ["/bin/promtool", "check", "config", "/etc/prometheus/prometheus.yml"]
