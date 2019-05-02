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

import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Base interface for aggregate functions determining the average of some value.<br>
 * This aggregate function uses a list of two property values for aggregation internally.
 * The list will contain two values:
 * <ol start=0>
 *   <li>The sum of all values considered by the average.</li>
 *   <li>The number of values added to the sum.</li>
 * </ol>
 * A post-processing step is necessary after the aggregation, to get the final average value.
 * The final value will be a {@code double} value or {@link PropertyValue#NULL_VALUE null},
 * if there were no elements aggregated (i.e. if the property was not set on any element).<p>
 * <b>Hint: </b> Implementations of this interface have to make sure to use a property value
 * with the correct format, as described above.
 */
public interface Average extends AggregateFunction {

  /**
   * The default value used internally in this aggregation.
   * Implementations of {@link #getIncrement(EPGMElement)} should return this value when the
   * element is ignored, i.e. when it does not have the attribute aggregated by this function.
   */
  PropertyValue IGNORED_VALUE = PropertyValue.create(
    Arrays.asList(PropertyValue.create(0L), PropertyValue.create(0L)));

  /**
   * The aggregation logic for calculating the average.
   * This function requires property values to have a certain format, see {@link Average}.
   *
   * @param aggregate previously aggregated value
   * @param increment value that is added to the aggregate
   * @return The new aggregate value.
   */
  @Override
  default PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
    List<PropertyValue> aggregateValue = aggregate.getList();
    List<PropertyValue> incrementValue = increment.getList();
    PropertyValue sum = PropertyValueUtils.Numeric.add(aggregateValue.get(0),
      incrementValue.get(0));
    PropertyValue count = PropertyValueUtils.Numeric.add(aggregateValue.get(1),
      incrementValue.get(1));
    aggregateValue.set(0, sum);
    aggregateValue.set(1, count);
    aggregate.setList(aggregateValue);
    return aggregate;
  }

  /**
   * Calculate the average from the internally used aggregate value.
   *
   * @param result The result of the aggregation step.
   * @return The average value (or null, if there were no elements to get the average of).
   * @throws IllegalArgumentException if the previous result had an invalid format.
   */
  @Override
  default PropertyValue postAggregate(PropertyValue result) {
    if (!Objects.requireNonNull(result).isList()) {
      throw new IllegalArgumentException("The aggregate value is expected to be a List.");
    }
    List<PropertyValue> value = result.getList();
    if (value.size() != 2) {
      throw new IllegalArgumentException("The aggregate value list is expected to have size 2.");
    }
    if (!value.get(0).isNumber() || !value.get(1).isLong()) {
      throw new IllegalArgumentException("The aggregate value list contains unsupported types.");
    }
    // Convert the two list values to a double.
    // The first was some unknown number type, the second a long.
    double sum = ((Number) value.get(0).getObject()).doubleValue();
    long count = value.get(1).getLong();
    if (count < 0) {
      throw new IllegalArgumentException("Invalid number of elements " + count +
        ", expected value greater than zero.");
    } else if (count == 0) {
      return PropertyValue.NULL_VALUE;
    } else {
      result.setDouble(sum / count);
      return result;
    }
  }
}
