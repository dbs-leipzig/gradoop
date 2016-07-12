#!/bin/bash

######################################################
# Script to generate multiple predictable data sets
######################################################

#Flink root directory
FLINK="/usr/local/flink-1.0.0"
#Used jar
JAR="gradoop-benchmark-0.2-SNAPSHOT.jar"
#Used generator running class
CLASS="org.gradoop.examples.datagen.PredictableTransactionsGeneratorRunner"
#Output directory in hdfs
OUTPUT="hdfs:///user/hduser/input/tlf/"

#Running commands
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -o ${OUTPUT} -mg -gc 1000 -gs 1
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -o ${OUTPUT} -mg -gc 10000 -gs 1
${FLINK}/bin/flink run -p 32 -c ${CLASS} ${JAR} -o ${OUTPUT} -mg -gc 100000 -gs 1


