FROM prom/alertmanager:v0.27.0
COPY monitoring/alertmanager-application.yml /etc/alertmanager/alertmanager.yml
RUN ["/bin/amtool", "check-config", "/etc/alertmanager/alertmanager.yml"]
