/**
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

package org.gradoop.common.storage.predicate.filter.impl;

import org.gradoop.common.storage.predicate.filter.api.ElementFilter;

/**
 * property larger than min filter
 *
 * @param <FilterImpl> filter implement type
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
  private final double min;

  /**
   * include min flag
   */
  private final boolean include;

  /**
   * property larger than constructor
   *
   * @param key property key
   * @param min property min value
   */
  public PropLargerThan(
    String key,
    double min
  ) {
    this(key, min, true);
  }

  /**
   * property larger than constructor
   *
   * @param key property key
   * @param min property min value
   * @param include include min value
   */
  public PropLargerThan(
    String key,
    double min,
    boolean include
  ) {
    this.key = key;
    this.min = min;
    this.include = include;
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

  protected double getMin() {
    return min;
  }

  protected boolean isInclude() {
    return include;
  }
}
