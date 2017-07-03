
package org.gradoop.flink.model.impl.operators.matching.common.query;

import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;

/**
 * A triple representation of a query edge.
 */
public class Triple {
  /**
   * The source vertex of that triple.
   */
  private final Vertex sourceVertex;
  /**
   * The edge of that triple.
   */
  private final Edge edge;
  /**
   * The target vertex of that triple.
   */
  private final Vertex targetVertex;

  /**
   * Creates a new triple using the specified query elements.
   *
   * @param sourceVertex source vertex
   * @param edge edge
   * @param targetVertex target vertex
   */
  public Triple(Vertex sourceVertex, Edge edge, Vertex targetVertex) {
    this.sourceVertex = sourceVertex;
    this.edge = edge;
    this.targetVertex = targetVertex;
  }

  public Vertex getSourceVertex() {
    return sourceVertex;
  }

  public Edge getEdge() {
    return edge;
  }

  public Vertex getTargetVertex() {
    return targetVertex;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Triple triple = (Triple) o;

    return edge != null ? edge.equals(triple.edge) : triple.edge == null;
  }

  @Override
  public int hashCode() {
    return edge != null ? edge.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "Triple{" + "sourceVertex=" + sourceVertex + ", edge=" + edge + ", targetVertex=" +
      targetVertex + '}';
  }
}
