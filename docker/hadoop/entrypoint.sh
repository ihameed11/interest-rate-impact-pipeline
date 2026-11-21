#!/bin/bash
# Starts ONE Hadoop role in this container.
# docker-compose.yml passes the role name as the first argument.
set -e

ROLE="$1"

case "$ROLE" in
  namenode)
    # The NameNode is the "index" of HDFS: it knows which machine holds
    # which piece of every file. On the very first start its storage
    # folder is empty and must be formatted once.
    if [ ! -d /data/namenode/current ]; then
      echo "First start: formatting the NameNode"
      hdfs namenode -format -force -nonInteractive
    fi
    exec hdfs namenode
    ;;
  datanode)
    # A DataNode stores the actual pieces (blocks) of the files.
    exec hdfs datanode
    ;;
  resourcemanager)
    # The ResourceManager is the "boss" of YARN: it decides which
    # machine runs which part of a job.
    exec yarn resourcemanager
    ;;
  nodemanager)
    # A NodeManager is a "worker" of YARN: it runs the map and
    # reduce tasks it is given.
    exec yarn nodemanager
    ;;
  *)
    # Anything else: run the command as given (useful for debugging).
    exec "$@"
    ;;
esac
