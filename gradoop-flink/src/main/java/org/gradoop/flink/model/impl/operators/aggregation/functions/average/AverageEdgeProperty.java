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
package org.gradoop.flink.model.impl.operators.aggregation.functions.average;

import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

/**
 * Calculates the average of a numberic property value of all edges.<br>
 * <i>Hint: </i> Call {@link FinishAverage} after using this aggregate function to get the final
 * aggregation result.
 */
public class AverageEdgeProperty extends AverageProperty implements EdgeAggregateFunction {

  /**
   * Creates a new instance of this average aggregate function.
   *
   * @param aggregatePropertyKey aggregate property key
   */
  public AverageEdgeProperty(String aggregatePropertyKey) {
    super(aggregatePropertyKey);
  }
}
