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
package org.gradoop.temporal.model.api.functions;

import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.Aggregation;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

/**
 * An aggregate function used by the {@link Aggregation} operator that handles {@link TemporalElement}s
 * separately.
 */
public interface TemporalAggregateFunction extends AggregateFunction {

  /**
   * Get the increment to be added to the aggregate from an element.
   * The default implementation of this method will check if the element is a temporal element,
   * in that case the increment will be determined by {@link #getIncrement(TemporalElement)}.
   * Otherwise the value of {@link #getNonTemporalIncrement(Element)} will be used.
   * <br>
   * <b>Note: </b> Implementations of the {@link TemporalAggregateFunction} interface should
   * implement {@link #getIncrement(TemporalElement)} (and optionally
   * {@link #getNonTemporalIncrement(Element)}) instead of this method.
   *
   * @param element element used to get the increment
   * @return The increment, possibly {@code null}.
   * @see #getIncrement(TemporalElement) method to be implemented instead of this method.
   */
  @Override
  default PropertyValue getIncrement(Element element) {
    if (element instanceof TemporalElement) {
      return getIncrement((TemporalElement) element);
    } else {
      return getNonTemporalIncrement(element);
    }
  }

  /**
   * Get the default value used for non-temporal elements or throw an exception.
   * By default, an {@link UnsupportedOperationException} is thrown.
   *
   * @param element The non-temporal element.
   * @return The default aggregate value.
   */
  default PropertyValue getNonTemporalIncrement(Element element) {
    throw new UnsupportedOperationException("This aggregate function only supports temporal elements.");
  }

  /**
   * Get the increment of a temporal element to be added to the aggregate value.
   *
   * @param element The temporal element.
   * @return The increment, possibly {@code null}.
   * @see AggregateFunction#getIncrement(Element) about this method.
   */
  PropertyValue getIncrement(TemporalElement element);
}
