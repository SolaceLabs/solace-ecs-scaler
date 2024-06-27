## Dockerfile for Solace ECS Service Scaler
## --> Requires config file mounted as volume to /opt/ecs-scaler/config.yaml

FROM eclipse-temurin:17

RUN mkdir -p /opt/ecs-scaler
RUN chmod 755 /opt/ecs-scaler
RUN mkdir -p /opt/ecs-scaler/healthz
WORKDIR /opt/ecs-scaler

ADD target/solace-ecs-scaler-*-jar-with-dependencies.jar ./solace-ecs-scaler.jar

ENTRYPOINT ["java", "-jar", "/opt/ecs-scaler/solace-ecs-scaler.jar", "--config-file=/opt/ecs-scaler/config.yaml"]

# Start Period here waits for the SolaceEcsAutoScalerApp initialization period (60s) to complete
HEALTHCHECK --interval=10s --start-period=75s --retries=3 CMD find /opt/ecs-scaler/healthz/health* | grep . || exit 1
