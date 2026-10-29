FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY extensions/pom.xml pom.xml
COPY extensions/src src
RUN mvn -B package dependency:copy-dependencies -DoutputDirectory=/src/lib
FROM apache/kafka:4.1.2
COPY --from=build /src/target/ticket-statistics-1.0.jar /lab/app.jar
COPY --from=build /src/lib/ /opt/kafka/libs/
COPY extensions/connect-worker.properties extensions/connect-file.properties /lab/
USER root
RUN apk add --no-cache libstdc++ \
    && mkdir -p /lab/data /opt/kafka/plugins/file \
    && mv /opt/kafka/libs/connect-file-4.1.2.jar /opt/kafka/plugins/file/ \
    && chown -R appuser:appuser /lab /opt/kafka/plugins
USER appuser
