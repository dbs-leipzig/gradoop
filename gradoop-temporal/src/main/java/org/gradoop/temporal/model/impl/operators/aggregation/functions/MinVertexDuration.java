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
package org.gradoop.temporal.model.impl.operators.aggregation.functions;

import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.temporal.model.api.TimeDimension;

/**
 * Calculates the minimum duration of all given vertices and the defined {@link TimeDimension}.
 */
public class MinVertexDuration extends MinDuration implements VertexAggregateFunction {

  /**
   * Creates a new instance of this base aggregate function.
   *
   * @param aggregatePropertyKey aggregate property key
   * @param dimension the time dimension to consider
   */
  public MinVertexDuration(String aggregatePropertyKey, TimeDimension dimension) {
    super(aggregatePropertyKey, dimension);
  }
}
