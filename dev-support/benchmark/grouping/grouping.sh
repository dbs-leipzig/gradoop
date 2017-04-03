#!/bin/bash

#Paralelism
PARA=""
#Number of repeats
ROUNDS=""
#Input Path (will be converted to hdfs path)
IN=""
#Output Path (will be converted to hdfs path)
OUT=""
#CSV output path (directory must be existing)
CSV=""
#Grouping Strategy
S=""
#Vertex grouping keys
VGK=""
#Edge grouping keys
EGK=""
#Use vertex labels
UVL=""
#Use edge labels
UEL=""
#Vertex aggregate functions
VAGG=""
#Vertex aggregate keys
VAK=""
#Vertex aggregate result keys
VARK=""
#Edge aggregate functions
EAGG=""
#Edge aggregate keys
EAK=""
#Edge aggregate result keys
EARK=""
#Used jar file
JAR_FILE=""
#Used benchmarking class
CLASS=""
#FLINK root directory
FLINK="$FLINK_PREFIX/bin/flink"
#HDFS root directory
HDFS="$HADOOP_PREFIX/bin/hadoop"

while read LINE
do 
[[ "$LINE" =~ ^#.*$ ]] && continue
LINE="$(echo -e "${LINE}" | tr -d '[[:space:]]')"
IFS=':' read -ra LINE <<< "$LINE"
KEY=${LINE[0]}

case ${KEY} in
    class)          CLASS="${LINE[1]}";;
    jar)            JAR_FILE="${LINE[1]}";;
    parallelism)    PARA="${LINE[1]}";;
    rounds)         ROUNDS="${LINE[1]}";;
    input)	        IN="${LINE[1]}";;
    output)	        OUT="${LINE[1]}";;
    csv)     	    CSV="${LINE[1]}";;
    strategy)       S="-s ${LINE[1]}";;
    vgk)	        VGK="-vgk ${LINE[1]}";;
    egk)	        EGK="-egk ${LINE[1]}";;
    uvl) 	        UVL="-uvl";;
    uel)	        UEL="-uel";;
    vagg)     	    VAGG="${LINE[1]}";;
    vak)	        VAK="-vak ${LINE[1]}";;
    vark)	        VARK="-vark ${LINE[1]}";;
    eagg)	        EAGG="${LINE[1]}";;
    eak)	        EAK="-eak ${LINE[1]]}";;
    eark)	        EARK="-eark ${LINE[1]}";
esac

done < grouping.conf

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
		    echo "Input: ${I}"
		    echo "Parallelism: ${P}"
		    echo "Strategy: ${S}"
 		    echo "========="
		    ${HDFS}/bin/hdfs dfs -rm -r ${OUT}
		    INPUT="hdfs://${I}"
		    OUTPUT="hdfs://${OUT}"
		    AGGS="-vagg ${VAGG} ${VAK} ${VARK} -eagg ${EAGG} ${EAK} ${EARK}"
		    ARGS="-csv ${CSV} ${S} ${UVL} ${UEL} ${VGK} ${EGK} ${AGGS}"
            ${FLINK}/bin/flink run -p ${P} -c ${CLASS} ${JAR_FILE} -i ${INPUT} -o ${OUTPUT} ${ARGS}
		done
	done
done
