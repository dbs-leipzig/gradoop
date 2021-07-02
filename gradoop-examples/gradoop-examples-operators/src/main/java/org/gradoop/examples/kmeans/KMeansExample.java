package org.gradoop.examples.kmeans;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.kmeans.KMeans;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class KMeansExample {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(GradoopFlinkConfig.createConfig(env));

        CSVDataSource csvDataSource = new CSVDataSource("/home/max/Documents/graphData/2018-citibike-csv-1/",
                GradoopFlinkConfig.createConfig(env));

        LogicalGraph cityBikeGraph = csvDataSource.getLogicalGraph();


        //GradoopFlinkConfig gradoopFlinkConfig = new GradoopFlinkConfig(env,)

        //MinimalCSVImporter minimalCSVImporter = new MinimalCSVImporter("./gradoop-examples/gradoop-examples-operators/src/main/resources/2018-citibike-csv-1/vertices.csv", ";",)

        //loader.initDatabaseFromFile(inputGraph.toString());

        //LogicalGraph cityBikeGraph = loader.getLogicalGraph();

        KMeans kmeans = new KMeans<>(3, 3);

        kmeans.execute(cityBikeGraph).toString();
    }
}

