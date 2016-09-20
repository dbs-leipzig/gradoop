#!/bin/bash

#Paralelism
PARALLELISMS=""
#Rounds of each testing
ROUNDS=""
#LOG output path (directory must be existing)
LOG=""
#Minimum Support param
THRESHOLD=""
#Flag is used dataset is a dmgthetic one
DIRECTED=""
#Flag if iterative iteration encoding should be used
IMPLEMENTATIONS=""
#Jar file witch should be used
JAR_FILE=""
#Class name of benchmark class
CLASS=""
#FLINK root directory
FLINK=""
#HDFS root directory
HDFS=""

while read LINE
do 
[[ "$LINE" =~ ^#.*$ ]] && continue
LINE="$(echo -e "${LINE}" | tr -d '[[:space:]]')"
IFS=':' read -ra LINE <<< "$LINE"
KEY=${LINE[0]}

case ${KEY} in
    flink_root)	    FLINK="${LINE[1]}";;
    hdfs_root)      HDFS="${LINE[1]}";;
    jar)            JAR_FILE="${LINE[1]}";;
    class)          CLASS="${LINE[1]}";;
    parallelism)    PARALLELISMS="${LINE[1]}";;
    rounds)         ROUNDS="${LINE[1]}";;
    input)	        DATASETS="${LINE[1]}";;
    output)	        OUT="${LINE[1]}";;
    log)            LOG="${LINE[1]}";;
    t)              THRESHOLDS="${LINE[1]}";;
    d)              DIRECTED="-d";;
    impl)           IMPLEMENTATIONS="${LINE[1]}";
esac

done < fsm.conf

IFS=',' 
read -ra PARALLELISMS <<< "$PARALLELISMS"
read -ra THRESHOLDS <<< "$THRESHOLDS"
read -ra DATASETS <<< "$DATASETS"
read -ra IMPLEMENTATIONS <<< "$IMPLEMENTATIONS"

unset IFS

for PARALLELISM in "${PARALLELISMS[@]}"
do
    for THRESHOLD in "${THRESHOLDS[@]}"
    do
        for DATASET in "${DATASETS[@]}"
        do
            for ((ROUND=1; ROUND<=$ROUNDS; ROUND++))
            do
                for IMPLEMENTATION in "${IMPLEMENTATIONS[@]}"
                do
                    echo "Benchmark"
                    echo "============================"
                    echo "Parallelism: ${PARALLELISM}"
                    echo "Threshold: ${THRESHOLD}"
                    echo "Dataset: ${DATASET}"
                    echo "Round: ${ROUND}"
                    echo "Implementation: ${IMPLEMENTATION}"
                    echo "============================"
#                    ${HDFS}/bin/hadoop dfs -rm -r ${OUT}
                    INPUT="hdfs://${DATASET}"
                    ARGS="-log ${LOG} -impl ${IMPLEMENTATION} ${DIRECTED}  -t ${THRESHOLD}"
                    ${FLINK}/bin/flink run -p ${PARALLELISM} -c ${CLASS} ${JAR_FILE} -i ${INPUT} ${ARGS}
                done
            done
        done
    done
done
