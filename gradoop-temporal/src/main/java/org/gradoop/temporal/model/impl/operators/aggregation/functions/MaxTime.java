/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
 * Base class for calculating the maximum of a field of a time-interval for temporal
 * elements. If the dimension {@link TimeDimension#VALID_TIME} is used for aggregation, this function ignores
 * the default values ({@link TemporalElement#DEFAULT_TIME_FROM} and {@link TemporalElement#DEFAULT_TIME_TO})
 * and handles it the same way as {@code null}.
 */
public class MaxTime extends AbstractTimeAggregateFunction {

  /**
   * Sets attributes used to initialize this aggregate function.
   *
   * @param dimension            The time dimension to consider.
   * @param field                The field of the time-interval to consider.
   */
  public MaxTime(TimeDimension dimension, TimeDimension.Field field) {
    this(dimension, field, String.format("max_%s_%s", dimension, field));
  }

  /**
   * Sets attributes used to initialize this aggregate function.
   *
   * @param dimension            The time dimension to consider.
   * @param field                The field of the time-interval to consider.
   * @param aggregatePropertyKey The key of the property where the aggregated result is saved.
   */
  public MaxTime(TimeDimension dimension, TimeDimension.Field field, String aggregatePropertyKey) {
    super(dimension, field, aggregatePropertyKey);
  }

  @Override
  public PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
    return applyAggregateWithDefaults(aggregate, increment, PropertyValueUtils.Numeric::max);
  }
}
