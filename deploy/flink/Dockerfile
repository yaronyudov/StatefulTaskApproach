# Flink image with the SQL connector JARs the pipeline needs (Kafka, OpenSearch, MongoDB).
# Versions track Flink 1.18; bump together if you change the base image.
FROM flink:1.18.1-scala_2.12-java11

ARG MAVEN=https://repo1.maven.org/maven2/org/apache/flink
ARG FK=3.1.0-1.18
ARG FOS=1.2.0-1.18
ARG FMG=1.2.0-1.18

RUN set -eux; \
    cd /opt/flink/lib; \
    curl -fSL -o flink-sql-connector-kafka.jar      ${MAVEN}/flink-sql-connector-kafka/${FK}/flink-sql-connector-kafka-${FK}.jar; \
    curl -fSL -o flink-sql-connector-opensearch.jar ${MAVEN}/flink-sql-connector-opensearch/${FOS}/flink-sql-connector-opensearch-${FOS}.jar; \
    curl -fSL -o flink-sql-connector-mongodb.jar    ${MAVEN}/flink-sql-connector-mongodb/${FMG}/flink-sql-connector-mongodb-${FMG}.jar
