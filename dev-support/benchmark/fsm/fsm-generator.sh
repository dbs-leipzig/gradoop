#!/bin/bash

######################################################
# File to run the fsm generator multiple times
######################################################

#Flink root directory
FLINK="/usr/local/flink-1.0.0"
#Used jar
JAR="fsm.jar"
#Used generator running class
CLASS="org.gradoop.examples.datagen.PredictableTransactionsGeneratorRunner"
#Output directory in hdfs
OUTPUT="hdfs:///user/hduser/input/datagen/"

#Running commands
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -o ${OUTPUT} -gc 100 -gs 1
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -o ${OUTPUT} -gc 1000 -gs 1
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -o ${OUTPUT} -gc 10000 -gs 1
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -o ${OUTPUT} -gc 100000 -gs 1
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -o ${OUTPUT} -gc 1000000 -gs 1
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -o ${OUTPUT} -gc 10000000 -gs 1

