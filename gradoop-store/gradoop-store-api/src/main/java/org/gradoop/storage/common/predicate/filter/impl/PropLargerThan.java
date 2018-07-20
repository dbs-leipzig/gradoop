/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.storage.common.predicate.filter.impl;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.storage.common.predicate.filter.api.ElementFilter;

/**
 * Predicate by property value compare
 * return element if and only if:
 *  - contains property key
 *  - property value type is comparable
 *  - property value is larger than min value
 *
 * @param <FilterImpl> filter implement type
 * @see PropertyValue#compareTo(PropertyValue) for more detail about value comparing
 */
public abstract class PropLargerThan<FilterImpl extends ElementFilter>
  implements ElementFilter<FilterImpl> {

  /**
   * property key
   */
  private final String key;

  /**
   * property value
   */
  private final PropertyValue min;

  /**
   * allow equality case
   */
  private final boolean include;

  /**
   * property larger than constructor
   *
   * @param key property key
   * @param min property min value
   * @param include include min value
   */
  public PropLargerThan(
    String key,
    Object min,
    boolean include
  ) {
    this.key = key;
    this.min = min instanceof PropertyValue ? (PropertyValue) min : PropertyValue.create(min);
    this.include = include;

    //noinspection EqualsWithItself, only for type check
    this.min.compareTo(this.min);
  }

  @Override
  public String toString() {
    return String.format("e.prop.%1$s IS NOT NULL AND e.prop.%1$s%3$s`%2$s`",
      key,
      min,
      include ? ">=" : ">");
  }

  protected String getKey() {
    return key;
  }

  protected PropertyValue getMin() {
    return min;
  }

  protected boolean isInclude() {
    return include;
  }

}
