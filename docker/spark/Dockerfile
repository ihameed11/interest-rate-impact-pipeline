# Spark image used by the Spark master and both Spark workers.
# Starts from the official Apache Spark image (Spark 3.5, Java 17, Python 3)
# and adds the PostgreSQL driver so Spark can write results into the database.

FROM spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# PostgreSQL JDBC driver: the "plug" that lets Spark talk to PostgreSQL.
# Anything placed in /opt/spark/jars is loaded by every Spark job.
ARG PG_JDBC_VERSION=42.7.4
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && curl -fSL "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar" \
      -o "/opt/spark/jars/postgresql-${PG_JDBC_VERSION}.jar"

# Tell Spark where the Hadoop settings are, so "hdfs://namenode:9000"
# works inside Spark jobs. The folder is mounted from config/hadoop.
ENV HADOOP_CONF_DIR=/opt/hadoop-conf
ENV PATH=$PATH:/opt/spark/bin
