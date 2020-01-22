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

import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.Objects;
import java.util.Set;

/**
 * A wrapper for aggregate functions that will only return the increment of elements that are not contained
 * in any label group.
 */
public class UnlabeledGroupAggregatorWrapper extends AggregatorWrapperWithValue {

  /**
   * A set of all labels where label-specific aggregators are defined.
   */
  private final Set<String> otherLabels;

  /**
   * Create a new instance of this wrapper.
   *
   * @param otherLabels A set of all labels where the aggregation function should <b>not</b> set used.
   * @param function    The aggregation function.
   * @param id          A {@code short} identifying this function internally.
   */
  public UnlabeledGroupAggregatorWrapper(Set<String> otherLabels, AggregateFunction function, short id) {
    super(function, PropertyValue.create(id));
    this.otherLabels = Objects.requireNonNull(otherLabels);
  }

  @Override
  public PropertyValue getIncrement(Element element) {
    return otherLabels.contains(element.getLabel()) ? PropertyValue.NULL_VALUE :
      wrap(wrappedFunction.getIncrement(element));
  }
}
