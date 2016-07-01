#!/bin/bash

PARA=""
ROUNDS=""
OUTPUT=""
CSV=""
VGK=""
EGK=""
UVL=""
UEL=""
VAGG=""
VAK=""
VARK=""
EAGG=""
EAK=""
EARK=""

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
    csv)     	 CSV="${LINE[1]}";;
    vgk)	 VGK="${LINE[1]}";;
    egk)	 EGK="${LINE[1]}";;
    uvl) 	 UVL="-uvl";;
    uel)	 UEL="-uel";;
    vagg)     	 VAGG="${LINE[1]}";;
    vak)	 VAK="-vak ${LINE[1]}";;
    vark)	 VARK="-vark ${LINE[1]}";;
    eagg)	 EAGG="${LINE[1]}";;
    eak)	 EAK="-eak ${LINE[1]]}";;
    eark)	 EARK="-eark ${LINE[1]}";
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
		    echo "OUTPUT: ${OUT}"
		    echo "PARALELLISM: ${P}"
 		    echo "========="
		    /usr/local/hadoop-2.5.2/bin/hdfs dfs -rm -r ${OUT}
		    INPUT="hdfs://${I}"
		    OUTPUT="hdfs://${OUT}"
		    AGGS="-vagg ${VAGG} ${VAK} ${VARK} -eagg ${EAGG} ${EAK} ${EARK}"
		    ARGS="-csv ${CSV} -vgk ${VGK} -egk ${EGK} ${AGGS}"
                    ${PREFIX} run -p ${P} -c ${CLASS} ${JAR_FILE} -i ${INPUT} -o ${OUTPUT} ${ARGS}
		done
	done
done
