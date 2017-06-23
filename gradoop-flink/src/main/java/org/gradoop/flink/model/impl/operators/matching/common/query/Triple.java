/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

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
