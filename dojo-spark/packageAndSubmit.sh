#!/bin/bash
set -e
set -x

CLASSNAME=$1
NAME=`hostname`
SPARK_IP=$SPARK_MASTER_IP
HDFS_IP=$SPARK_MASTER_IP

if [ -z "$CLASSNAME" ]
then
  echo 'Classname must be passed as first parameter'
  exit 1
fi

if [ -z "$NAME" ]
then
  echo 'Variable $NAME must be defined'
  exit 1
fi

if [ -z "$SPARK_IP" ]
then
  echo 'Variable $SPARK_MASTER_IP must be defined'
  exit 1
fi

if [ -z "$HDFS_IP" ]
then
  echo 'Variable $HDFS_IP must be defined'
  exit 1
fi

mvn package

hdfs dfs -rm -f /jars/dojo-spark-0.0.1-SNAPSHOT-$NAME.jar
hdfs dfs -mkdir -p /jars
hdfs dfs -put target/dojo-spark-0.0.1-SNAPSHOT.jar /jars/dojo-spark-0.0.1-SNAPSHOT-$NAME.jar

$SPARK_HOME/bin/spark-submit \
  --class $CLASSNAME \
  --master spark://$SPARK_IP:7077 \
  --deploy-mode cluster \
  hdfs://$HDFS_IP:9000/jars/dojo-spark-0.0.1-SNAPSHOT-$NAME.jar