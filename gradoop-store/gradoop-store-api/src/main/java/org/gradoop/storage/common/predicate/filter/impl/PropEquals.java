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

import javax.annotation.Nonnull;

/**
 * Predicate by property equality
 * return element if and only if:
 *  - contains property key
 *  - property value equals to given value
 *
 * @param <FilterImpl> filter implement type
 */
public abstract class PropEquals<FilterImpl extends ElementFilter>
  implements ElementFilter<FilterImpl> {

  /**
   * property key
   */
  private final String key;

  /**
   * property value
   */
  private final PropertyValue value;

  /**
   * property equals filter
   *
   * @param key property key
   * @param value property value
   */
  public PropEquals(
    @Nonnull String key,
    @Nonnull Object value
  ) {
    this.key = key;
    this.value = value instanceof PropertyValue ?
      (PropertyValue) value : PropertyValue.create(value);
  }

  @Override
  public String toString() {
    return String.format("e.prop.%1$s IS NOT NULL AND e.prop.%1$s=`%2$s`",
      key, value);
  }

  protected String getKey() {
    return key;
  }

  protected PropertyValue getValue() {
    return value;
  }

}
