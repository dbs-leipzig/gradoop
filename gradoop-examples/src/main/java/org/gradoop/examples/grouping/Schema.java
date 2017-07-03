
package org.gradoop.examples.grouping;

import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import static java.util.Collections.singletonList;
import static org.gradoop.flink.model.impl.operators.grouping.Grouping.LABEL_SYMBOL;

/**
 * Demo program that uses graph grouping to extract the schema from a (possibly large) property
 * (logical) graph. The resulting summary graph is written to DOT for easy visualization.
 */
public class Schema extends AbstractRunner {

  /**
   * Loads the graph from the specified input path, computes its schema via grouping and writes
   * the result as DOT into the output path.
   *
   * args[0] - input path (CSV)
   * args[1] - output path
   *
   * @param args arguments
   * @throws Exception if something goes wrong
   */
  public static void main(String[] args) throws Exception {
    String inputPath = args[0];
    String outputPath = args[1];

    // instantiate a default gradoop config
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());

    // define a data source to load the graph
    DataSource dataSource = new CSVDataSource(inputPath, config);

    // load the graph
    LogicalGraph graph = dataSource.getLogicalGraph();

    // use graph grouping to extract the schema
    LogicalGraph schema = graph.groupBy(singletonList(LABEL_SYMBOL), singletonList(LABEL_SYMBOL));

    // instantiate a data sink for the DOT format
    DataSink dataSink = new DOTDataSink(outputPath, false);
    dataSink.write(schema, true);

    // run the job
    getExecutionEnvironment().execute();
  }
}
