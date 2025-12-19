FROM apache/spark:3.5.1

USER root

ARG POSTGRES_JDBC_VERSION=42.7.4
ARG CLICKHOUSE_JDBC_VERSION=0.6.0
ARG CASSANDRA_CONNECTOR_VERSION=3.5.1

RUN apt-get update \
  && apt-get install -y --no-install-recommends curl ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir \
  cassandra-driver==3.29.2 \
  neo4j==5.18.0 \
  pymongo==4.6.3 \
  redis==5.0.4

RUN set -eux; \
  curl -fsSL -o "/opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
  curl -fsSL -o "/opt/spark/jars/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar" \
    "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar"; \
  curl -fsSL -o "/opt/spark/jars/spark-cassandra-connector-assembly_2.12-${CASSANDRA_CONNECTOR_VERSION}.jar" \
    "https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/${CASSANDRA_CONNECTOR_VERSION}/spark-cassandra-connector-assembly_2.12-${CASSANDRA_CONNECTOR_VERSION}.jar"

ENV PATH="/opt/spark/bin:${PATH}"

USER spark
