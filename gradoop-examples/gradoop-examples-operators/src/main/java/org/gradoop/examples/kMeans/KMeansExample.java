package org.gradoop.examples.kMeans;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.graph.GraphCsvReader;
import org.gradoop.dataintegration.importer.impl.csv.MinimalCSVImporter;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class KMeansExample {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(GradoopFlinkConfig.createConfig(env));

        CsvReader inputGraph = env.readCsvFile("./gradoop-examples/gradoop-examples-operators/src/main/resources/2018-citibike-csv-1/vertices.csv");

        //GradoopFlinkConfig gradoopFlinkConfig = new GradoopFlinkConfig(env,)

        //MinimalCSVImporter minimalCSVImporter = new MinimalCSVImporter("./gradoop-examples/gradoop-examples-operators/src/main/resources/2018-citibike-csv-1/vertices.csv", ";",)

        loader.initDatabaseFromFile(inputGraph.toString());

        LogicalGraph cityBikeGraph = loader.getLogicalGraph();

        KMeans kmeans = new KMeans<>(3, 10);

        kmeans.execute(cityBikeGraph).toString();
    }
}
