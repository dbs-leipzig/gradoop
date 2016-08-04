package org.gradoop.io.impl.gelly;

import org.apache.flink.graph.Graph;
import org.gradoop.io.api.DataSink;
import org.gradoop.io.impl.gelly.functions.ToGellyEdge;
import org.gradoop.io.impl.gelly.functions.ToGellyVertex;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.id.GradoopId;

import java.io.IOException;

public class GellyGraphDataSink
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements DataSink<G, V, E> {

  private final Graph<GradoopId, V, E> graph;

  public GellyGraphDataSink(final Graph<GradoopId, V, E> graph) {
    this.graph = graph;
  }

  @Override
  public void write(LogicalGraph<G, V, E> logicalGraph) throws IOException {
    write(GraphCollection.fromGraph(logicalGraph));
  }

  @Override
  public void write(GraphCollection<G, V, E> graphCollection) throws
    IOException {
    try {
      graph.addVertices(graphCollection.getVertices()
        .map(new ToGellyVertex<V>())
        .collect());

      graph.addEdges(graphCollection.getEdges()
        .map(new ToGellyEdge<E>())
        .collect());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void write(GraphTransactions<G, V, E> graphTransactions) throws
    IOException {
    write(GraphCollection.fromTransactions(graphTransactions));
  }
}
