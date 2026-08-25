# syntax=docker/dockerfile:1.7
FROM maven:3.9.11-eclipse-temurin-21-alpine AS build

WORKDIR /workspace
COPY pom.xml ./
COPY order-service/pom.xml order-service/pom.xml
COPY notification-service/pom.xml notification-service/pom.xml
RUN mvn -B -ntp -pl order-service -am dependency:go-offline

COPY order-service/src order-service/src
RUN mvn -B -ntp -pl order-service -am package -DskipTests

FROM eclipse-temurin:21-jre-alpine

RUN apk add --no-cache wget \
    && addgroup -S orderflow \
    && adduser -S orderflow -G orderflow

WORKDIR /app
COPY --from=build --chown=orderflow:orderflow /workspace/order-service/target/order-service-*.jar app.jar

USER orderflow
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=5 \
    CMD wget -qO- http://localhost:8080/actuator/health/readiness || exit 1

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-XX:+UseG1GC", "-jar", "/app/app.jar"]
