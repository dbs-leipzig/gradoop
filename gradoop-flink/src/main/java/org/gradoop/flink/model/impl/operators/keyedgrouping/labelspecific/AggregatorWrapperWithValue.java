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
package org.gradoop.flink.model.impl.operators.keyedgrouping.labelspecific;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateDefaultValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Wraps an aggregate function where the internal representation of a {@link PropertyValue} is a {@link List}
 * containing some property value and the original aggregate value.
 * The additional value is used to identify aggregate values used by this function.
 */
abstract class AggregatorWrapperWithValue implements AggregateFunction, AggregateDefaultValue {

  /**
   * The actual aggregate function.
   */
  protected final AggregateFunction wrappedFunction;

  /**
   * The additional value stored with the property value.
   * This value is used to identify values to be aggregated.
   */
  private final PropertyValue identifyingValue;

  /**
   * Create a new instance of this wrapper.
   *
   * @param wrappedFunction The wrapped aggregate function.
   * @param identifyingValue The additional value used to identify values to be aggregated.
   */
  AggregatorWrapperWithValue(AggregateFunction wrappedFunction, PropertyValue identifyingValue) {
    this.wrappedFunction = Objects.requireNonNull(wrappedFunction);
    this.identifyingValue = Objects.requireNonNull(identifyingValue);
  }

  /**
     * Unwrap a {@link PropertyValue} wrapped by this class.
     *
     * @param wrappedValue The wrapped property value.
     * @return The unwrapped (raw) property value.
     * @see #wrap(PropertyValue)
     */
  protected PropertyValue unwrap(PropertyValue wrappedValue) {
    if (wrappedValue.isNull()) {
      return wrappedValue;
    }
    return wrappedValue.getList().get(1);
  }

  /**
     * Wrap a {@link PropertyValue} into an internal representation used by this class.
     *
     * @param rawValue The raw property value.
     * @return The wrapped property value.
     * @see #unwrap(PropertyValue)
     */
  protected PropertyValue wrap(PropertyValue rawValue) {
    return PropertyValue.create(Arrays.asList(identifyingValue, rawValue));
  }

  /**
     * Check if a {@link PropertyValue} should be considered by this function.
     *
     * @param value The property value.
     * @return {@code true}, if the value should be aggregated by this function.
     */
  protected boolean isAggregated(PropertyValue value) {
    if (!value.isList()) {
      return false;
    }
    final List<PropertyValue> values = value.getList();
    if (values.size() != 2) {
      return false;
    }
    return identifyingValue.equals(values.get(0));
  }

  @Override
  public PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
    if (isAggregated(increment)) {
      return wrap(wrappedFunction.aggregate(unwrap(aggregate), unwrap(increment)));
    } else {
      return aggregate;
    }
  }

  @Override
  public String getAggregatePropertyKey() {
    return wrappedFunction.getAggregatePropertyKey();
  }

  @Override
  public PropertyValue postAggregate(PropertyValue result) {
    if (isAggregated(result)) {
      return wrappedFunction.postAggregate(unwrap(result));
    } else {
      return null;
    }
  }

  @Override
  public PropertyValue getDefaultValue() {
    return wrappedFunction instanceof AggregateDefaultValue ?
      ((AggregateDefaultValue) wrappedFunction).getDefaultValue() : PropertyValue.NULL_VALUE;
  }
}
