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

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

/**
 * Base class for calculating the minimum of a field of a time-interval for temporal
 * elements. If a valid time attribute is used for aggregation, this function ignores the default
 * values ({@link TemporalElement#DEFAULT_TIME_FROM} and {@link TemporalElement#DEFAULT_TIME_TO})
 * and handles it the same way as {@code null}.
 */
public class MinTime extends AbstractTimeAggregateFunction {

  /**
   * Sets attributes used to initialize this aggregate function.
   *
   * @param aggregatePropertyKey The aggregate property key.
   * @param dimension            The time dimension to consider.
   * @param field                The field of the time-interval to consider.
   */
  public MinTime(String aggregatePropertyKey, TimeDimension dimension, TimeDimension.Field field) {
    super(aggregatePropertyKey, dimension, field);
  }

  @Override
  public PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
    return applyAggregateWithDefaults(aggregate, increment, PropertyValueUtils.Numeric::min);
  }
}
