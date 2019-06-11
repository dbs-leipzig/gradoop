/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.combination;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.BinaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;

/**
 * Computes the combined graph from two logical graphs.
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The type of the graph.
 * @param <GC> The type of the graph collection.
 */
public class Combination<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  implements BinaryBaseGraphToBaseGraphOperator<LG> {

  /**
   * Creates a new logical graph by union the vertex and edge sets of two
   * input graphs. Vertex and edge equality is based on their respective identifiers.
   *
   * @param firstGraph  first input graph
   * @param secondGraph second input graph
   * @return combined graph
   */
  @Override
  public LG execute(LG firstGraph, LG secondGraph) {

    DataSet<V> newVertexSet = firstGraph.getVertices()
      .union(secondGraph.getVertices())
      .distinct(new Id<>());

    DataSet<E> newEdgeSet = firstGraph.getEdges()
      .union(secondGraph.getEdges())
      .distinct(new Id<>());

    return firstGraph.getFactory().fromDataSets(newVertexSet, newEdgeSet);
  }

}
