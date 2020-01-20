/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.equality;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.BinaryBaseGraphCollectionToValueOperator;
import org.gradoop.flink.model.impl.functions.bool.Equals;
import org.gradoop.flink.model.impl.operators.tostring.CanonicalAdjacencyMatrixBuilder;
import org.gradoop.flink.model.impl.operators.tostring.api.EdgeToString;
import org.gradoop.flink.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.flink.model.impl.operators.tostring.api.VertexToString;

/**
 * Operator to determine if two graph collections are equal according to given
 * string representations of graph heads, vertices and edges.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> type of the base graph instance
 * @param <GC> type of the graph collection
 */
public class CollectionEquality<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  implements BinaryBaseGraphCollectionToValueOperator<GC, DataSet<Boolean>> {

  /**
   * builder to create the string representations of graph collections used for comparison.
   */
  private final CanonicalAdjacencyMatrixBuilder<G, V, E, LG, GC> canonicalAdjacencyMatrixBuilder;

  /**
   * Constructor to set string representations.
   *
   * @param graphHeadToString string representation of graph heads
   * @param vertexToString string representation of vertices
   * @param edgeToString string representation of edges
   * @param directed sets mode for directed or undirected graphs
   */
  public CollectionEquality(GraphHeadToString<G> graphHeadToString,
    VertexToString<V> vertexToString, EdgeToString<E> edgeToString, boolean directed) {
    // sets mode for directed or undirected graphs
    this.canonicalAdjacencyMatrixBuilder = new CanonicalAdjacencyMatrixBuilder<>(
      graphHeadToString, vertexToString, edgeToString, directed);
  }

  @Override
  public DataSet<Boolean> execute(GC firstCollection, GC secondCollection) {
    return Equals.cross(
      canonicalAdjacencyMatrixBuilder.execute(firstCollection),
      canonicalAdjacencyMatrixBuilder.execute(secondCollection)
    );
  }
}
