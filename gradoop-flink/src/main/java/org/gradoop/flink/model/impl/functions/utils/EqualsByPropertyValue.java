/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.functions.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * A map function returning true, as a long as a certain property has as equal value for all input
 * elements.
 *
 * @param <E> Type of the Element with Properties.
 */
public class EqualsByPropertyValue<E extends Element> implements MapFunction<E, Boolean> {

  /**
   * Property key to read.
   */
  private final String propertyKey;

  /**
   * Value of the first element.
   */
  private PropertyValue firstValue;

  /**
   * Initialize.
   *
   * @param propertyKey Property key to read.
   */
  public EqualsByPropertyValue(String propertyKey) {
    this.propertyKey = propertyKey;
    this.firstValue = null;
  }

  @Override
  public Boolean map(E e) throws Exception {
    if (firstValue == null) {
      firstValue = e.getPropertyValue(propertyKey);
      return Boolean.TRUE;
    }
    return firstValue.equals(e.getPropertyValue(propertyKey));
  }
}
