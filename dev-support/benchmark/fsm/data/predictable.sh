#!/bin/bash

######################################################
# Script to generate multiple predictable data sets
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
CLASS="org.gradoop.examples.datagen.PredictableTransactionsGeneratorRunner"

# create graph count data sets
for SIZE in 1000 3000 10000
do
  ${FLINK}/bin/flink run -p ${PARA} -c ${CLASS} ${JAR} -o hdfs://${HDFS}/ -mg -gc ${SIZE} -gs 1
done

for SIZE in 2 3
do
  ${FLINK}/bin/flink run -p ${PARA} -c ${CLASS} ${JAR} -o hdfs://${HDFS}/ -mg -gc 1000 -gs ${SIZE}
done


