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
package org.gradoop.temporal.model.impl.operators.aggregation.functions;

import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.temporal.model.api.TimeDimension;

/**
 * Aggregates the minimum value of a time value for temporal edges.
 * The value will be calculated as the minimum of a {@link TimeDimension.Field} of a
 * {@link TimeDimension}, ignoring the default value (in this case {@link Long#MIN_VALUE}).
 */
public class MinEdgeTime extends MinTime implements EdgeAggregateFunction {

  /**
   * Creates an instance of the {@link MinEdgeTime} aggregate function.
   *
   * @param aggregatePropertyKey The aggregate property key.
   * @param dimension            The time dimension to consider.
   * @param field                The field of the time-interval to consider.
   */
  public MinEdgeTime(String aggregatePropertyKey, TimeDimension dimension, TimeDimension.Field field) {
    super(aggregatePropertyKey, dimension, field);
  }
}
