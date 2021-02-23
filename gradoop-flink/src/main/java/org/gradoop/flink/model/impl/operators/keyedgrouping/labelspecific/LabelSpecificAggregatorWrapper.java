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

/**
 * A wrapper for an aggregate function that will only return the increment of elements that have a certain
 * label.
 */
public class LabelSpecificAggregatorWrapper extends AggregatorWrapperWithValue {

  /**
   * The expected label of the elements to aggregate.
   */
  private final String targetLabel;

  /**
   * Create a new instance of this wrapper.
   *
   * @param targetLabel     The expected label.
   * @param wrappedFunction The aggregate function to be used for elements with this label.
   * @param id              A {@code short} identifying this function internally.
   */
  public LabelSpecificAggregatorWrapper(String targetLabel, AggregateFunction wrappedFunction, short id) {
    super(wrappedFunction, PropertyValue.create(id));
    this.targetLabel = Objects.requireNonNull(targetLabel);
  }

  @Override
  public PropertyValue getIncrement(Element element) {
    return element.getLabel().equals(targetLabel) ?
      wrap(wrappedFunction.getIncrement(element)) :
      PropertyValue.NULL_VALUE;
  }

}
