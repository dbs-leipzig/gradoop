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

import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.ReducibleBinaryGraphToGraphOperator;

/**
 * Computes the combined graph from a collection of logical graphs.
 */
public class ReduceCombination implements ReducibleBinaryGraphToGraphOperator {

  /**
   * Creates a new logical graph by union the vertex and edge sets of all graph
   * contained in the given collection.
   *
   * @param collection input collection
   * @return combined graph
   */
  @Override
  public LogicalGraph execute(GraphCollection collection) {
    return collection.getConfig().getLogicalGraphFactory().fromDataSets(
      collection.getVertices(), collection.getEdges());
  }

  @Override
  public String getName() {
    return ReduceCombination.class.getName();
  }
}
