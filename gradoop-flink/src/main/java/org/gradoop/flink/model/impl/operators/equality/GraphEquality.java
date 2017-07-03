package org.gradoop.flink.model.impl.operators.equality;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.api.operators.BinaryGraphToValueOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.operators.tostring.api.EdgeToString;
import org.gradoop.flink.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.flink.model.impl.operators.tostring.api.VertexToString;

/**
 * Operator to determine if two graph are equal according to given string
 * representations of graph heads, vertices and edges.
 */
public class GraphEquality implements BinaryGraphToValueOperator<Boolean> {

  /**
   * collection equality operator, wrapped by graph equality
   * (graph are considered to be a 1-graph collection)
   */
  private final CollectionEquality collectionEquality;
  /**
   * sets mode for directed or undirected graphs
   */
  private final boolean directed;

  /**
   * constructor to set string representations
   * @param graphHeadToString string representation of graph heads
   * @param vertexToString string representation of vertices
   * @param edgeToString string representation of edges
   * @param directed sets mode for directed or undirected graphs
   */
  public GraphEquality(GraphHeadToString<GraphHead> graphHeadToString,
    VertexToString<Vertex> vertexToString, EdgeToString<Edge> edgeToString,
    boolean directed) {
    this.directed = directed;

    this.collectionEquality = new CollectionEquality(
      graphHeadToString, vertexToString, edgeToString, this.directed);
  }

  @Override
  public DataSet<Boolean> execute(
    LogicalGraph firstGraph, LogicalGraph secondGraph) {
    return collectionEquality.execute(
      GraphCollection.fromGraph(firstGraph),
      GraphCollection.fromGraph(secondGraph)
    );
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
