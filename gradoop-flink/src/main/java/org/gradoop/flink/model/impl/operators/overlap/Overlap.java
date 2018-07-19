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
package org.gradoop.flink.model.impl.operators.overlap;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;

/**
 * Computes the overlap graph from two logical graphs.
 */
public class Overlap implements BinaryGraphToGraphOperator {

  /**
   * Creates a new logical graph containing the overlapping vertex and edge
   * sets of two input graphs. Vertex and edge equality is based on their
   * respective identifiers.
   *
   * @param firstGraph  first input graph
   * @param secondGraph second input graph
   * @return graph with overlapping elements from both input graphs
   */
  @Override
  public LogicalGraph execute(
    LogicalGraph firstGraph, LogicalGraph secondGraph) {

    DataSet<Vertex> newVertices = firstGraph.getVertices()
      .join(secondGraph.getVertices())
      .where(new Id<>())
      .equalTo(new Id<>())
      .with(new LeftSide<>());

    DataSet<Edge> newEdges = firstGraph.getEdges()
      .join(secondGraph.getEdges())
      .where(new Id<>())
      .equalTo(new Id<>())
      .with(new LeftSide<>());

    return firstGraph.getConfig().getLogicalGraphFactory().fromDataSets(newVertices, newEdges);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Overlap.class.getName();
  }


}
