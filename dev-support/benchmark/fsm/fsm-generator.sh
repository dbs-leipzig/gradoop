#!/bin/bash

######################################################
# File to run the fsm generator multiple times
######################################################

FLINK="/usr/local/flink-1.0.0"
JAR="fsm.jar"
CLASS="org.gradoop.examples.datagen.PredictableTransactionsGeneratorRunner"
OUTPUT="hdfs:///user/hduser/input/datagen/"

${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -o ${OUTPUT} -gc 100 -gs 1
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -o ${OUTPUT} -gc 1000 -gs 1
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -o ${OUTPUT} -gc 10000 -gs 1
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -o ${OUTPUT} -gc 100000 -gs 1
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -o ${OUTPUT} -gc 1000000 -gs 1
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -o ${OUTPUT} -gc 10000000 -gs 1

