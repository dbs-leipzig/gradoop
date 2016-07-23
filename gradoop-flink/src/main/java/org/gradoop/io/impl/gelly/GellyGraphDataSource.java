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

public class GellyGraphDataSource
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements DataSource<G, V, E> {

  private final Graph<GradoopId, V, E> graph;

  private final G graphHead;

  private final GradoopFlinkConfig<G, V, E> config;

  public GellyGraphDataSource(final Graph<GradoopId, V, E> graph,
    final G graphHead,
    final GradoopFlinkConfig<G, V, E> config) {
    this.graph = graph;
    this.graphHead = graphHead;
    this.config = config;
  }

  public GellyGraphDataSource(final Graph<GradoopId, V, E> graph,
    final GradoopFlinkConfig<G, V, E> config) {
    this(graph, null, config);
  }

  @Override
  public LogicalGraph<G, V, E> getLogicalGraph() throws IOException {
    DataSet<V> vertices = graph.getVertices()
      .map(new MapFunction<Vertex<GradoopId, V>, V>() {
        @Override
        public V map(Vertex<GradoopId, V> gellyVertex) throws Exception {
          return gellyVertex.getValue();
        }
      }).withForwardedFields("f1->*");

    DataSet<E> edges = graph.getEdges()
      .map(new MapFunction<Edge<GradoopId, E>, E>() {
        @Override
        public E map(Edge<GradoopId, E> gellyEdge) throws Exception {
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
