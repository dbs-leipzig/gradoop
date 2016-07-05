#!/bin/bash

#Paralelism
PARA=""
#Rounds of each testing
ROUNDS=""
#LOG output path (directory must be existing)
LOG=""
#Minimum Support param
T=""
#Flag is used dataset is a dmgthetic one
DIRECTED=""
#Flag if iterative iteration encoding should be used
IMPL=""
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
    flink_root)	 FLINK="${LINE[1]}";;
    hdfs_root)   HDFS="${LINE[1]}";;
    jar)     JAR_FILE="${LINE[1]}";;
    class)   CLASS="${LINE[1]}";;
    parallelism) PARA="${LINE[1]}";;
    rounds)      ROUNDS="${LINE[1]}";;
    input)	 IN="${LINE[1]}";;
    output)	 OUT="${LINE[1]}";;
    log)     LOG="${LINE[1]}";;
    t)      T="${LINE[1]}";;
    d)     DIRECTED="-dmg";;
    impl)    IMPL="-iterative";
esac

done < fsm.conf

IFS=',' 
read -ra PARA <<< "$PARA"
read -ra IN <<< "$IN"

unset IFS

for P in "${PARA[@]}"
do
	for I in "${IN[@]}"
	do
		for ((R=1; R<=$ROUNDS; R++))
		do
		    echo "Benchmark"
		    echo "========="
		    echo "INPUT: ${I}"
		    echo "PARALLELISM: ${P}"
 		    echo "========="
		    ${HDFS}/bin/hadoop dfs -rm -r ${OUT}
		    INPUT="hdfs://${I}"
		    ARGS="-log ${LOG} -impl ${IMPL} ${DIRECTED}  -t ${T}"
            ${FLINK}/bin/flink run -p ${P} -c ${CLASS} ${JAR_FILE} -i ${INPUT} ${ARGS}
		done
	done
done
