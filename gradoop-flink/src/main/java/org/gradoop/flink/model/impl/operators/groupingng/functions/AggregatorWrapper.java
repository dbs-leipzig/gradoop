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
package org.gradoop.flink.model.impl.operators.groupingng.functions;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateDefaultValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.Objects;

/**
 * An abstract base class for all functions wrapping an aggregate function.
 */
abstract class AggregatorWrapper implements AggregateFunction, AggregateDefaultValue {

  /**
   * The actual aggregate function.
   */
  protected final AggregateFunction wrappedFunction;

  /**
   * Create a new instance of this wrapper.
   *
   * @param wrappedFunction The wrapped aggregate function.
   */
  AggregatorWrapper(AggregateFunction wrappedFunction) {
    this.wrappedFunction = Objects.requireNonNull(wrappedFunction);
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

  /**
   * Unwrap a {@link PropertyValue} wrapped by this class.
   *
   * @param wrappedValue The wrapped property value.
   * @return The unwrapped (raw) property value.
   * @see #wrap(PropertyValue)
   */
  protected PropertyValue unwrap(PropertyValue wrappedValue) {
    return wrappedValue;
  }

  /**
   * Wrap a {@link PropertyValue} into an internal representation used by this class.
   *
   * @param rawValue The raw property value.
   * @return The wrapped property value.
   * @see #unwrap(PropertyValue)
   */
  protected PropertyValue wrap(PropertyValue rawValue) {
    return rawValue;
  }

  /**
   * Check if a {@link PropertyValue} should be considered by this function.
   *
   * @param value The property value.
   * @return {@code true}, if the value should be aggregated by this function.
   */
  protected boolean isAggregated(PropertyValue value) {
    return true;
  }
}
