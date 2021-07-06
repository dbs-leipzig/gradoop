package org.gradoop.examples.kmeans;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.kmeans.KMeans;;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class KMeansExample {

    public static void main(String[] args){

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        CSVDataSource csvDataSource = new CSVDataSource("/home/max/Documents/graphData/2018-citibike-csv-1/",
                GradoopFlinkConfig.createConfig(env));

        LogicalGraph cityBikeGraph = csvDataSource.getLogicalGraph();

        KMeans kmeans = new KMeans<>(3, 3, "lat", "long");

        kmeans.execute(cityBikeGraph).toString();
    }
}

