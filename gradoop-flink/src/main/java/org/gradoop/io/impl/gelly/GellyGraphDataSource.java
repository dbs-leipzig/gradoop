package org.gradoop.io.impl.gelly;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.io.api.DataSource;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * DataSource for Gelly Graphs
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GellyGraphDataSource
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements DataSource<G, V, E> {

  /**
   * Gelly Graph
   */
  private final Graph<GradoopId, V, E> graph;

  /**
   * EPGM graph head for the logical graph
   */
  private final G graphHead;

  /**
   * Gradoop Flink Configuration
   */
  private final GradoopFlinkConfig<G, V, E> config;

  /**
   * Creates a logical graph from the given Gelly graph. All vertices and edges
   * will be assigned to the given graph head.
   *
   * @param graph     Flink Gelly graph
   * @param graphHead EPGM graph head for the logical graph
   * @param config    Gradoop Flink configuration
   */
  public GellyGraphDataSource(final Graph<GradoopId, V, E> graph,
    final G graphHead,
    final GradoopFlinkConfig<G, V, E> config) {
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
  public GellyGraphDataSource(final Graph<GradoopId, V, E> graph,
    final GradoopFlinkConfig<G, V, E> config) {
    this(graph, null, config);
  }

  @Override
  public LogicalGraph<G, V, E> getLogicalGraph() throws IOException {
    DataSet<V> vertices = graph.getVertices()
      .map(new MapFunction<Vertex<GradoopId, V>, V>() {
        @Override
        public V map(final Vertex<GradoopId, V> gellyVertex) throws Exception {
          return gellyVertex.getValue();
        }
      }).withForwardedFields("f1->*");

    DataSet<E> edges = graph.getEdges()
      .map(new MapFunction<Edge<GradoopId, E>, E>() {
        @Override
        public E map(final Edge<GradoopId, E> gellyEdge) throws Exception {
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
  public GraphCollection<G, V, E> getGraphCollection() throws IOException {
    return GraphCollection.fromGraph(getLogicalGraph());
  }

  @Override
  public GraphTransactions<G, V, E> getGraphTransactions() throws IOException {
    return getGraphCollection().toTransactions();
  }
}
