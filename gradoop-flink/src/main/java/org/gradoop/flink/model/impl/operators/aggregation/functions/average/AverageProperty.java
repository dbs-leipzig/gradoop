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
import org.gradoop.flink.model.api.functions.AggregateDefaultValue;
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Base class for aggregate functions determining the average of a numeric property value.<br>
 * This aggregate function uses a list of two property values for aggregation internally.
 * The list will contain two values:
 * <ol start=0>
 *   <li>The sum all values considered by the average.</li>
 *   <li>The number of values added to the sum.</li>
 * </ol>
 * A post-processing step is necessary, after the aggregation, to get the final average value.
 * The final value will be a {@code double} value or {@link PropertyValue#NULL_VALUE null},
 * if there were no elements aggregated (i.e. if the property was not set on any element).
 */
public abstract class AverageProperty extends BaseAggregateFunction
  implements AggregateDefaultValue {

  /**
   * A property value containing the number {@code 1}, as a {@code long}.
   */
  protected static final PropertyValue ONE = PropertyValue.create(1L);

  /**
   * The default value used in this aggregation.
   */
  private final PropertyValue defaultValue = PropertyValue.create(
    Arrays.asList(PropertyValue.create(0L), PropertyValue.create(0L)));

  /**
   * The key used to read the value to aggregate from.
   */
  private final String propertyKey;

  /**
   * Creates a new instance of a base average aggregate function with a default aggregate property
   * key (will be the original property key with prefix {@code avg_}).
   *
   * @param propertyKey The key of the property to aggregate.
   */
  public AverageProperty(String propertyKey) {
    this(propertyKey, "avg_" + propertyKey);
  }

  /**
   * Creates a new instance of a base average aggregate function.
   *
   * @param propertyKey          The key of the property to aggregate.
   * @param aggregatePropertyKey The propertyKey used to store the aggregate.
   */
  public AverageProperty(String propertyKey, String aggregatePropertyKey) {
    super(aggregatePropertyKey);
    this.propertyKey = Objects.requireNonNull(propertyKey);
  }

  @Override
  public PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
    List<PropertyValue> aggregateValue = validateAndGetValue(aggregate);
    List<PropertyValue> incrementValue = validateAndGetValue(increment);
    PropertyValue sum = PropertyValueUtils.Numeric.add(aggregateValue.get(0),
      incrementValue.get(0));
    PropertyValue count = PropertyValueUtils.Numeric.add(aggregateValue.get(1),
      incrementValue.get(1));
    aggregateValue.set(0, sum);
    aggregateValue.set(1, count);
    return PropertyValue.create(aggregateValue);
  }

  @Override
  public PropertyValue getDefaultValue() {
    return defaultValue;
  }

  @Override
  public PropertyValue getIncrement(EPGMElement element) {
    PropertyValue value = element.getPropertyValue(propertyKey);
    return value == null ? defaultValue : asInternalAggregate(value);
  }

  /**
   * Calculate the average from the internally used aggregate value.
   *
   * @param result The result of the aggregation step.
   * @return The average value (or null, if the there were no element to get the average of).
   */
  @Override
  public PropertyValue postAggregate(PropertyValue result) {
    List<PropertyValue> value = validateAndGetValue(result);
    // Convert the two list values to a double.
    // The first was some unknown number type, the second a long.
    double sum = ((Number) value.get(0).getObject()).doubleValue();
    double count = (double) value.get(1).getLong();
    if (count < 0) {
      throw new IllegalArgumentException("Invalid number of elements " + count + ", expected " +
        "value greater than zero.");
    } else if (count == 0) {
      return PropertyValue.NULL_VALUE;
    } else {
      return PropertyValue.create(sum / count);
    }
  }

  /**
   * Transform a numeric property value to the internal list-based representation of the aggregate
   * value.
   *
   * @param value The property value of an numeric type.
   * @return A list-type property value containing the input value and a count ({@code 1L}).
   */
  protected static PropertyValue asInternalAggregate(PropertyValue value) {
    if (!value.isNumber()) {
      throw new IllegalArgumentException("Property value has to be a number.");
    }
    return PropertyValue.create(Arrays.asList(value, ONE));
  }

  /**
   * Check if a property has the correct type used internally in this aggregate function
   * and return the value. Otherwise an {@link IllegalArgumentException} will be thrown.
   *
   * @param value The property value.
   * @return A list containing the actual values used in the aggregation.
   * @throws IllegalArgumentException when the value does not have the correct type or format.
   */
  static List<PropertyValue> validateAndGetValue(PropertyValue value) {
    Objects.requireNonNull(value);
    if (!value.isList()) {
      throw new IllegalArgumentException("Property value is not a list: " + value);
    }
    List<PropertyValue> valueList = value.getList();
    if (valueList.size() != 2) {
      throw new IllegalArgumentException("Property value list does not have the expected size.");
    }
    if (!valueList.get(0).isNumber() || !valueList.get(1).isLong()) {
      throw new IllegalArgumentException("Property values do not have supported types.");
    }
    return valueList;
  }
}
