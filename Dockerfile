## Dockerfile for Solace ECS Service Scaler
## --> Requires config file mounted as volume to /opt/ecs-scaler/config.yaml

FROM eclipse-temurin:17

RUN mkdir -p /opt/ecs-scaler
RUN chmod 777 /opt/ecs-scaler
WORKDIR /opt/ecs-scaler

ADD target/solace-ecs-scaler-*-jar-with-dependencies.jar ./solace-ecs-scaler.jar

ENTRYPOINT ["java", "-jar", "/opt/ecs-scaler/solace-ecs-scaler.jar", "--config-file=/opt/ecs-scaler/config.yaml"]
