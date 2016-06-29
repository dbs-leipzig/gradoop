#!/bin/bash

PARA=""
ROUNDS=""
CSV=""
MS=""
SYN=""
BULK=""

JAR_FILE="gradoop-examples-0.2-SNAPSHOT.jar"
CLASS="org.gradoop.examples.benchmark.GroupingBenchmark"

while read LINE
do 
[[ "$LINE" =~ ^#.*$ ]] && continue
LINE="$(echo -e "${LINE}" | tr -d '[[:space:]]')"
IFS=':' read -ra LINE <<< "$LINE"
KEY=${LINE[0]}

case ${KEY} in
    prefix)	 PREFIX="${LINE[1]}";;
    parallelism) PARA="${LINE[1]}";;
    rounds)      ROUNDS=${LINE[1]};;
    input)	 IN="${LINE[1]}";;
    output)	 OUT="${LINE[1]}";;
    csv)     CSV="${LINE[1]}";;
    ms)      MS="${LINE[1]}";;
    syn)     SYN="-syn";;
    bulk)    BULK="-bulk";;
esac

done < grouping_conf

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
		    /usr/local/hadoop-2.5.2/bin/hdfs dfs -rm -r ${OUT}
		    INPUT="hdfs://${I}"
		    ARGS="-csv ${CSV} -ms ${MS} ${SYN} ${BULK}"
            ${PREFIX} run -p ${P} -c ${CLASS} ${JAR_FILE} -i ${INPUT} ${ARGS}
		done
	done
done
