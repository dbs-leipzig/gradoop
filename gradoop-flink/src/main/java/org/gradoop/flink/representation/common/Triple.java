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

package org.gradoop.flink.representation.common;


import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * This class represents a Triple.
 * A Triple represents an edge extended with information about the source and target vertex.
 */
public class Triple extends Tuple3<Vertex,Edge, Vertex> {

  /**
   * Creates a new Triple
   * @param sourceVertex source vertex
   * @param edge edge
   * @param targetVertex target vertex
   */
  public Triple(Vertex sourceVertex, Edge edge, Vertex targetVertex) {
    super(sourceVertex, edge, targetVertex);
    requireValidTriple(sourceVertex, edge, targetVertex);
  }

  /**
   * Returns the source vertex.
   * @return source vertex
   */
  public Vertex getSourceVertex() {
    return f0;
  }

  /**
   * returns the edge
   * @return edge
   */
  public Edge getEdge() {
    return f1;
  }

  /**
   * Returns the target vertex
   * @return target vertex
   */
  public Vertex getTargetVertex() {
    return f2;
  }

  /**
   * Ensures the validity of a triple. Requires that sourceVertex.id = edge.sourceId and
   * targetVertex.id = edge.targetId
   * @param sourceVertex source vertex
   * @param edge edge
   * @param targetVertex target vertex
   */
  private static void requireValidTriple(Vertex sourceVertex, Edge edge, Vertex targetVertex) {
    if (sourceVertex.getId() != edge.getSourceId()) {
      throw new IllegalArgumentException("Source IDs do not match");
    }

    if (targetVertex.getId() != edge.getTargetId()) {
      throw new IllegalArgumentException("Target IDs do not match");
    }
  }
}
