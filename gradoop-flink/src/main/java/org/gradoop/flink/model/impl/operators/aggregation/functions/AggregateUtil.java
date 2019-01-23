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

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateDefaultValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.Map;
import java.util.Set;

/**
 * Utility functions for the aggregation operator
 */
public class AggregateUtil {

  /**
   * Increments the aggregate map by the increment of the aggregate functions on the element
   *
   * @param aggregate aggregate map to be incremented
   * @param element element to increment with
   * @param aggregateFunctions aggregate functions
   * @return incremented aggregate map
   */
  static Map<String, PropertyValue> increment(Map<String, PropertyValue> aggregate, Element element,
                                              Set<AggregateFunction> aggregateFunctions) {
    for (AggregateFunction aggFunc : aggregateFunctions) {
      PropertyValue increment = aggFunc.getIncrement(element);
      if (increment != null) {
        aggregate.compute(aggFunc.getAggregatePropertyKey(), (key, agg) -> agg == null ?
          increment.copy() : aggFunc.aggregate(agg, increment));
      }
    }
    return aggregate;
  }

  /**
   * Returns the default aggregate value for the given aggregate function
   * or {@link PropertyValue#NULL_VALUE}, if it has no default.
   *
   * @param aggregateFunction aggregate function
   * @return aggregate value
   */
  public static PropertyValue getDefaultAggregate(AggregateFunction aggregateFunction) {
    if (aggregateFunction instanceof AggregateDefaultValue) {
      return ((AggregateDefaultValue) aggregateFunction).getDefaultValue();
    } else {
      return PropertyValue.NULL_VALUE;
    }
  }
}
