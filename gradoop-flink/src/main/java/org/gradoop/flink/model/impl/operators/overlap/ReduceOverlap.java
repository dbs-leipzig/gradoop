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
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.ReducibleBinaryGraphToGraphOperator;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Computes the overlap graph from a collection of logical graphs.
 */
public class ReduceOverlap extends OverlapBase implements
  ReducibleBinaryGraphToGraphOperator {

  /**
   * Creates a new logical graph containing the overlapping vertex and edge sets
   * of the graphs contained in the given collection. Vertex and edge equality
   * is based on their respective identifiers.
   *
   * @param collection input collection
   * @return graph with overlapping elements from the input collection
   */
  @Override
  public LogicalGraph execute(GraphCollection collection) {
    DataSet<GraphHead> graphHeads = collection.getGraphHeads();

    DataSet<GradoopId> graphIDs = graphHeads.map(new Id<GraphHead>());

    return collection.getConfig().getLogicalGraphFactory().fromDataSets(
      getVertices(collection.getVertices(), graphIDs),
      getEdges(collection.getEdges(), graphIDs)
    );
  }

  @Override
  public String getName() {
    return ReduceOverlap.class.getName();
  }
}
