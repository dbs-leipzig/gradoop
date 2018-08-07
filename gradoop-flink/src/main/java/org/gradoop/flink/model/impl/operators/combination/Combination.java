/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;

/**
 * Computes the combined graph from two logical graphs.
 */
public class Combination implements BinaryGraphToGraphOperator {

  /**
   * Creates a new logical graph by union the vertex and edge sets of two
   * input graphs. Vertex and edge equality is based on their respective
   * identifiers.
   *
   * @param firstGraph  first input graph
   * @param secondGraph second input graph
   * @return combined graph
   */
  @Override
  public LogicalGraph execute(LogicalGraph firstGraph,
    LogicalGraph secondGraph) {

    DataSet<Vertex> newVertexSet = firstGraph.getVertices()
      .union(secondGraph.getVertices())
      .distinct(new Id<Vertex>());

    DataSet<Edge> newEdgeSet = firstGraph.getEdges()
      .union(secondGraph.getEdges())
      .distinct(new Id<Edge>());

    return firstGraph.getConfig().getLogicalGraphFactory().fromDataSets(newVertexSet, newEdgeSet);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Combination.class.getName();
  }

}
