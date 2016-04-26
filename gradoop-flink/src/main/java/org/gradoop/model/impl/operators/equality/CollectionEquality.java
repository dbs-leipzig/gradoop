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
package org.gradoop.model.impl.operators.equality;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryCollectionToValueOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.operators.tostring.CanonicalAdjacencyMatrixBuilder;
import org.gradoop.model.impl.operators.tostring.api.EdgeToString;
import org.gradoop.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.model.impl.operators.tostring.api.VertexToString;

/**
 * Operator to determine if two graph collections are equal according to given
 * string representations of graph heads, vertices and edges.
 * @param <G> graph head type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class CollectionEquality
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements BinaryCollectionToValueOperator<G, V, E, Boolean> {

  /**
   * builder to create the string representations of graph collections used for
   * comparison.
   */
  private final CanonicalAdjacencyMatrixBuilder
    <G, V, E> canonicalAdjacencyMatrixBuilder;
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
  public CollectionEquality(GraphHeadToString<G> graphHeadToString,
    VertexToString<V> vertexToString, EdgeToString<E> edgeToString,
    boolean directed) {
    this.directed = directed;
    this.canonicalAdjacencyMatrixBuilder =
      new CanonicalAdjacencyMatrixBuilder<>(
        graphHeadToString, vertexToString, edgeToString, this.directed);
  }

  @Override
  public DataSet<Boolean> execute(GraphCollection<G, V, E> firstCollection,
    GraphCollection<G, V, E> secondCollection) {
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
