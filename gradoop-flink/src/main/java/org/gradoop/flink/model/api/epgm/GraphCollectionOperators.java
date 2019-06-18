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
package org.gradoop.flink.model.api.epgm;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.util.Order;
import org.gradoop.flink.model.api.operators.ApplicableUnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

/**
 * Defines the operators that are available on a {@link GraphCollection}.
 */
public interface GraphCollectionOperators extends GraphBaseOperators {

  //----------------------------------------------------------------------------
  // Logical Graph / Graph Head Getters
  //----------------------------------------------------------------------------

  /**
   * Returns logical graph from collection using the given identifier. If the
   * graph does not exist, an empty logical graph is returned.
   *
   * @param graphID graph identifier
   * @return logical graph with given id or an empty logical graph
   */
  LogicalGraph getGraph(final GradoopId graphID);
  /**
   * Extracts logical graphs from collection using their identifiers.
   *
   * @param identifiers graph identifiers
   * @return collection containing requested logical graphs
   */
  GraphCollection getGraphs(final GradoopId... identifiers);

  /**
   * Extracts logical graphs from collection using their identifiers.
   *
   * @param identifiers graph identifiers
   * @return collection containing requested logical graphs
   */
  GraphCollection getGraphs(GradoopIdSet identifiers);

  //----------------------------------------------------------------------------
  // Auxiliary operators
  //----------------------------------------------------------------------------

  /**
   * Applies a given unary graph to graph operator (e.g., aggregate) on each
   * logical graph in the graph collection.
   *
   * @param op applicable unary graph to graph operator
   * @return collection with resulting logical graphs
   */
  GraphCollection apply(
    ApplicableUnaryGraphToGraphOperator op);
}
