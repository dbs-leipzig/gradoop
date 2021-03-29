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
package org.gradoop.flink.model.impl.operators.aggregation.functions.min;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.flink.model.api.functions.AggregateFunction;

/**
 * Interface of aggregate functions that determine a minimal value.<br>
 * This aggregation supports numerical property value types (short, int, long, float, double, big decimal) and
 * date and datetime types.
 */
public interface Min extends AggregateFunction {

  @Override
  default PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
    // If both values are of either type date or datetime, use the corresponding utility class.
    if (PropertyValueUtils.Date.isDateOrDateTime(aggregate) &&
      PropertyValueUtils.Date.isDateOrDateTime(increment)) {
      return PropertyValueUtils.Date.min(aggregate, increment);
    }
    return PropertyValueUtils.Numeric.min(aggregate, increment);
  }
}
