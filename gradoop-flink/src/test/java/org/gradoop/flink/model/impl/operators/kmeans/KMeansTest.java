package org.gradoop.flink.model.impl.operators.kmeans;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class KMeansTest extends GradoopFlinkTestBase {
    // init execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create loader
    FlinkAsciiGraphLoader loader = getLoaderFromString("(a:Coordinates {lat:'36', lon:'40'}) (b:Coordinates {lat:'27', lon:'45'}) (c:Coordinates {lat:'40', lon:'37'}) (d:Coordinates {lat:'45', lon:'38'}))");

    LogicalGraph logicalGraph = loader.getLogicalGraph();

    KMeans kMeans = new KMeans<>(2,2);


}
