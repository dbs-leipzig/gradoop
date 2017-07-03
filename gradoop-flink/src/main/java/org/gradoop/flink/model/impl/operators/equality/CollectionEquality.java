package org.gradoop.flink.model.impl.operators.equality;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.api.operators.BinaryCollectionToValueOperator;
import org.gradoop.flink.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.flink.model.impl.operators.tostring.api.VertexToString;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.bool.Equals;
import org.gradoop.flink.model.impl.operators.tostring.CanonicalAdjacencyMatrixBuilder;
import org.gradoop.flink.model.impl.operators.tostring.api.EdgeToString;

/**
 * Operator to determine if two graph collections are equal according to given
 * string representations of graph heads, vertices and edges.
 */
public class CollectionEquality
  implements BinaryCollectionToValueOperator<Boolean> {

  /**
   * builder to create the string representations of graph collections used for
   * comparison.
   */
  private final CanonicalAdjacencyMatrixBuilder canonicalAdjacencyMatrixBuilder;

  /**
   * constructor to set string representations
   * @param graphHeadToString string representation of graph heads
   * @param vertexToString string representation of vertices
   * @param edgeToString string representation of edges
   * @param directed sets mode for directed or undirected graphs
   */
  public CollectionEquality(GraphHeadToString<GraphHead> graphHeadToString,
    VertexToString<Vertex> vertexToString, EdgeToString<Edge> edgeToString,
    boolean directed) {
    /*
    sets mode for directed or undirected graphs
   */
    this.canonicalAdjacencyMatrixBuilder = new CanonicalAdjacencyMatrixBuilder(
        graphHeadToString, vertexToString, edgeToString, directed);
  }

  @Override
  public DataSet<Boolean> execute(GraphCollection firstCollection,
    GraphCollection secondCollection) {
    return Equals.cross(
      canonicalAdjacencyMatrixBuilder.execute(firstCollection),
      canonicalAdjacencyMatrixBuilder.execute(secondCollection)
    );
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
