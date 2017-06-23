#!/usr/bin/env bash

# example script that runs data generator, data copier and finally DIMSpan
# put JAR file in the same directory as this script

# configuration
HDFS="/opt/hadoop/bin/hdfs"
FLINK="/opt/flink-1.1.2/bin/flink"
DIR="hdfs:///dimspan"
JAR="gradoop-examples-0.3.0-SNAPSHOT.jar"

# create temp HDFS directory
${HDFS} dfs -mkdir ${DIR}

# generate synthetic dataset
${FLINK} run -c org.gradoop.examples.dimspan.SyntheticDataGenerator ${JAR} -c 100 -o ${DIR}/g100.tlf

# put real dataset to HDFS
# ${HDFS} dfs -put real.tlf ${DIR}/real.tlf

# scale up (use for real data)
${FLINK} run -c org.gradoop.examples.dimspan.TLFDataCopier ${JAR} -c 10 -i ${DIR}/g100.tlf -o ${DIR}/g1000.tlf

# run DIMSpan (directed and undirected mode)
${FLINK} run -c org.gradoop.examples.dimspan.DIMSpanRunner ${JAR} -ms 1.0 -i ${DIR}/g1000.tlf -o ${DIR}/directed.tlf
${FLINK} run -c org.gradoop.examples.dimspan.DIMSpanRunner ${JAR} -ms 1.0 -i ${DIR}/g1000.tlf -u -o ${DIR}/undirected.tlf

# copy result to local FS
/opt/hadoop/bin/hdfs dfs -get /dimspan/directed.tlf .
/opt/hadoop/bin/hdfs dfs -get /dimspan/undirected.tlf .

# drop temp dir
${HDFS} dfs -rm -r ${DIR}
