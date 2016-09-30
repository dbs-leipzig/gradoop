#!/bin/bash

######################################################
# Script to duplicate TLF data multiple times
######################################################

#Flink root directory
FLINK="/usr/local/flink-1.1.2"
#Used jar
JAR="gradoop-examples-0.2-SNAPSHOT.jar"

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

