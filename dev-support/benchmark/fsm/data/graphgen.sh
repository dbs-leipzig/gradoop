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
#Input directory in hdfs
INPUT="hdfs:///user/hduser/input/tlf/yeast.tlf"

#Running commands
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -i ${INPUT} -m 10
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -i ${INPUT} -m 100
