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
package org.gradoop.storage.impl.accumulo.predicate.filter.impl;

import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.storage.common.predicate.filter.impl.PropLargerThan;
import org.gradoop.storage.impl.accumulo.predicate.filter.api.AccumuloElementFilter;

/**
 * Accumulo property value compare predicate implement
 *
 * @param <T> EPGM element type
 */
public class AccumuloPropLargerThan<T extends EPGMElement>
  extends PropLargerThan<AccumuloElementFilter<T>>
  implements AccumuloElementFilter<T> {

  /**
   * Create a new property compare filter
   *
   * @param key property key
   * @param min property min value
   * @param include include min value
   */
  public AccumuloPropLargerThan(
    String key,
    Object min,
    boolean include
  ) {
    super(key, min, include);
  }

  @Override
  public boolean test(T t) {
    PropertyValue value = t.getPropertyValue(getKey());
    if (value == null) {
      return false;
    }

    try {
      int result = value.compareTo(getMin());
      return isInclude() ? result >= 0 : result > 0;
    } catch (UnsupportedOperationException | IllegalArgumentException typeErr) {
      return false;
    }
  }

}
