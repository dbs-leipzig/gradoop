#!/bin/bash

######################################################
# Script to duplicate TLF data multiple times
######################################################

#Flink directory
FLINK=$1
#Used jar
JAR=$2
#Hadoop directory
HADOOP=$3
#HDFS PATH
HDFS=$4
#Parallelism
PARA=$5
#Used generator running class
CLASS="org.gradoop.benchmark.fsm.TLFDataDuplicator"


#Running commands

for SIZE in 1 10
do
  ${FLINK}/bin/flink run -p ${PARA} -c ${CLASS} ${JAR} -i hdfs://${HDFS}/yeast.tlf -m ${SIZE}
done