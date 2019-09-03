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
package org.gradoop.temporal.model.api;

import org.gradoop.flink.model.api.operators.BaseGraphCollectionOperatorSupport;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;

/**
 * Defines the operators that are available on a {@link TemporalGraphCollection}.
 */
public interface TemporalGraphCollectionOperators
  extends BaseGraphCollectionOperatorSupport<TemporalGraph, TemporalGraphCollection> {

  //----------------------------------------------------------------------------
  // Utilities
  //----------------------------------------------------------------------------

  /**
   * Converts the {@link TemporalGraphCollection} to a {@link GraphCollection} instance by
   * discarding all temporal information from the graph elements. All Ids (graphs, vertices,
   * edges) are kept during the transformation.
   *
   * @return the graph collection instance
   */
  GraphCollection toGraphCollection();
}
