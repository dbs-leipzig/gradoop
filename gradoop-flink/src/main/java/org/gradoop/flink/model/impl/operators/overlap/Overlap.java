/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Computes the overlap graph from two logical graphs.
 */
public class Overlap extends OverlapBase implements BinaryGraphToGraphOperator {

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

    DataSet<GradoopId> graphIds = firstGraph.getGraphHead()
      .map(new Id<GraphHead>())
      .union(secondGraph.getGraphHead().map(new Id<GraphHead>()));

    return LogicalGraph.fromDataSets(
      getVertices(firstGraph.getVertices(), graphIds),
      getEdges(firstGraph.getEdges(), graphIds),
      firstGraph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Overlap.class.getName();
  }


}
