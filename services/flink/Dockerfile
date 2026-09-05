ARG FLINK_BASE_IMAGE=flink:1.20.3-scala_2.12-java11

FROM maven:3.9.9-eclipse-temurin-11 AS build
ARG FLINK_KAFKA_CONNECTOR_VERSION=3.4.0-1.20
WORKDIR /build
COPY services/flink/pom.xml .
COPY services/flink/src ./src
RUN mvn -Dflink.kafka.connector.version=${FLINK_KAFKA_CONNECTOR_VERSION} -DskipTests package

FROM ${FLINK_BASE_IMAGE}
COPY --from=build /build/target/delivery-flink-job.jar /opt/flink/usrlib/delivery-flink-job.jar
COPY configs/platform.yaml /opt/deliveryflow/configs/platform.yaml
