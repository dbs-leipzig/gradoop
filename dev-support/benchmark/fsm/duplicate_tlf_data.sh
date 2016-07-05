#!/bin/bash

######################################################
# Script to duplicate TLF data multiple times
######################################################

#Flink root directory
FLINK="/usr/local/flink-1.0.0"
#Used jar
JAR="gradoop-benchmark-0.2-SNAPSHOT.jar"
#Used generator running class
CLASS="org.gradoop.examples.datagen.TLFDataDuplicator"

#Running commands
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -m 10
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -m 100

