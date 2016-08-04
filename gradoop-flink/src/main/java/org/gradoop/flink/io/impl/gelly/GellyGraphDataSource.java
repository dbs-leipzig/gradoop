package org.gradoop.flink.io.impl.gelly;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * DataSource for Gelly Graphs
 *
 */
public class GellyGraphDataSource implements DataSource {

  /**
   * Gelly Graph
   */
  private final Graph<GradoopId, Vertex, Edge> graph;

  /**
   * EPGM graph head for the logical graph
   */
  private final GraphHead graphHead;

  /**
   * Gradoop Flink Configuration
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a logical graph from the given Gelly graph. All vertices and edges
   * will be assigned to the given graph head.
   *
   * @param graph     Flink Gelly graph
   * @param graphHead EPGM graph head for the logical graph
   * @param config    Gradoop Flink configuration
   */
  public GellyGraphDataSource(final Graph<GradoopId, Vertex, Edge> graph,
    final GraphHead graphHead,
    final GradoopFlinkConfig config) {
    this.graph = graph;
    this.graphHead = graphHead;
    this.config = config;
  }

  /**
   * Creates a logical graph from the given Gelly graph. A new graph head will
   * created, all vertices and edges will be assigned to that logical graph.
   *
   * @param graph     Flink Gelly graph
   * @param config    Gradoop Flink configuration
   */
  public GellyGraphDataSource(final Graph<GradoopId, Vertex, Edge> graph,
    final GradoopFlinkConfig config) {
    this(graph, null, config);
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    DataSet<Vertex> vertices = graph.getVertices()
      .map(new MapFunction<org.apache.flink.graph.Vertex<GradoopId, Vertex>,
          Vertex>() {
        @Override
        public Vertex map(final org.apache.flink.graph.Vertex<GradoopId,
          Vertex> gellyVertex)
          throws Exception {
          return gellyVertex.getValue();
        }
      }).withForwardedFields("f1->*");

    DataSet<Edge> edges = graph.getEdges()
      .map(new MapFunction<org.apache.flink.graph.Edge<GradoopId, Edge>,
        Edge>() {
        @Override
        public Edge map(final org.apache.flink.graph.Edge<GradoopId, Edge>
          gellyEdge)
          throws Exception {
          return gellyEdge.getValue();
        }
      }).withForwardedFields("f2->*");

    if (graphHead == null) {
      return LogicalGraph.fromDataSets(vertices, edges, config);
    } else {
      return LogicalGraph.fromDataSets(config.getExecutionEnvironment()
        .fromElements(graphHead),
        vertices, edges, config);
    }
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    return GraphCollection.fromGraph(getLogicalGraph());
  }

  @Override
  public GraphTransactions getGraphTransactions() throws IOException {
    return getGraphCollection().toTransactions();
  }
}
