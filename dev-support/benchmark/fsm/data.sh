#!/bin/bash

######################################################
# Script to duplicate TLF data multiple times
######################################################

#Flink root directory
FLINK="/usr/local/flink-1.0.0"
#Used jar
JAR="gradoop-benchmark-0.2-SNAPSHOT.jar"
#Used generator running class
CLASS="org.gradoop.benchmark.fsm.TLFDataDuplicator"

HDFS="/user/hduser/input"

PARA=32

HADOOP="$HADOOP_PREFIX/bin/hdfs"

#Running commands
${HADOOP} dfs -rm -r ${HDFS}
${HADOOP} dfs -mkdir ${HDFS}
${HADOOP} dfs -put yeast.tlf ${HDFS}/yeast.tlf
${HADOOP} dfs -ls ${HDFS}/

./data/yeast.sh "${FLINK}" "${JAR}" "${HADOOP}" "${HDFS}" ${PARA}
./data/predictable.sh "${FLINK}" "${JAR}" "${HADOOP}" "${HDFS}" ${PARA}

${HADOOP} dfs -ls ${HDFS}/

