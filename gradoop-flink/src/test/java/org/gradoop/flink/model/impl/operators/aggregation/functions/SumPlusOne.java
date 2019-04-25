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
package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumVertexProperty;

/**
 * An aggregate function used for aggregation tests. This function extends the sum aggregate
 * function by a post-processing step incrementing the result by {@code 1L}.
 * The expected result of this aggregation is the same as {@link SumVertexProperty} ({@code +1}).
 * This function can therefore be used to check that the post-processing function is run once
 * and only once.
 */
public class SumPlusOne extends SumVertexProperty {

  /**
   * Create an instance of this test function.
   *
   * @param propertyKey          The property key to aggregate.
   * @param aggregatePropertyKey The property key used to store the result.
   */
  public SumPlusOne(String propertyKey, String aggregatePropertyKey) {
    super(propertyKey, aggregatePropertyKey);
  }

  /**
   * Post-processing for this aggregate function: Increment the result by {@code 1}.
   *
   * @param result The result of the aggregation step.
   * @return The final result.
   */
  @Override
  public PropertyValue postAggregate(PropertyValue result) {
    return PropertyValueUtils.Numeric.add(result, PropertyValue.create(1L));
  }
}
