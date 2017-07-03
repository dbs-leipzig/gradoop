
package org.gradoop.flink.io.impl.csv;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.functions.CSVEdgeToEdge;
import org.gradoop.flink.io.impl.csv.functions.CSVLineToVertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A graph data source for CSV files.
 */
public class CSVDataSource extends CSVBase implements DataSource {

  /**
   * Creates a new CSV data source.
   *
   * @param csvPath path to the directory containing the CSV files
   * @param config Gradoop Flink configuration
   */
  public CSVDataSource(String csvPath, GradoopFlinkConfig config) {
    super(csvPath, config);
  }

  @Override
  public LogicalGraph getLogicalGraph() {
    DataSet<Tuple2<String, String>> metaData = readMetaData(getMetaDataPath());

    DataSet<Vertex> vertices = getConfig().getExecutionEnvironment()
      .readTextFile(getVertexCSVPath())
      .map(new CSVLineToVertex(getConfig().getVertexFactory()))
      .withBroadcastSet(metaData, BC_METADATA);

    DataSet<Edge> edges = getConfig().getExecutionEnvironment()
      .readTextFile(getEdgeCSVPath())
      .map(new CSVEdgeToEdge(getConfig().getEdgeFactory()))
      .withBroadcastSet(metaData, BC_METADATA);

    return LogicalGraph.fromDataSets(vertices, edges, getConfig());
  }

  @Override
  public GraphCollection getGraphCollection() {
    return GraphCollection.fromGraph(getLogicalGraph());
  }

  @Override
  public GraphTransactions getGraphTransactions() {
    return getGraphCollection().toTransactions();
  }
}
