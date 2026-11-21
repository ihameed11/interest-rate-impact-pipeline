# Hadoop image used by every Hadoop machine in the cluster
# (NameNode, DataNodes, ResourceManager, NodeManagers).
# One image, different role per container (see entrypoint.sh).

# Java 11 on Ubuntu 22.04. Hadoop is a Java program.
FROM eclipse-temurin:11-jdk-jammy

ARG HADOOP_VERSION=3.4.1

ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=$PATH:/opt/hadoop/bin:/opt/hadoop/sbin

# python3: our MapReduce mapper and reducer are Python scripts,
#          so every machine that runs them needs Python.
# curl, procps: small tools for downloading and for checking processes.
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 curl procps \
 && rm -rf /var/lib/apt/lists/*

# Download Hadoop from the Apache archive and unpack it into /opt/hadoop.
# The file is large (close to 1 GB), so the first build takes a while.
RUN curl -fSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o /tmp/hadoop.tar.gz \
 && tar -xzf /tmp/hadoop.tar.gz -C /opt \
 && mv "/opt/hadoop-${HADOOP_VERSION}" /opt/hadoop \
 && rm /tmp/hadoop.tar.gz \
 && rm -rf /opt/hadoop/share/doc

# Our own settings replace Hadoop's default (empty) ones.
COPY config/hadoop/*.xml /opt/hadoop/etc/hadoop/

# Folders where HDFS keeps its data inside each container.
RUN mkdir -p /data/namenode /data/datanode

COPY docker/hadoop/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
