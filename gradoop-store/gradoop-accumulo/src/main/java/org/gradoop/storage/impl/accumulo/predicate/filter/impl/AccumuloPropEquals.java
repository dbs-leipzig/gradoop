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
import org.gradoop.storage.common.predicate.filter.impl.PropEquals;
import org.gradoop.storage.impl.accumulo.predicate.filter.api.AccumuloElementFilter;

import javax.annotation.Nonnull;

/**
 * Accumulo property equality implement
 *
 * @param <T> EPGM element type
 */
public class AccumuloPropEquals<T extends EPGMElement>
  extends PropEquals<AccumuloElementFilter<T>>
  implements AccumuloElementFilter<T> {

  /**
   * Create a new property equals filter
   *
   * @param key property key
   * @param value property value
   */
  public AccumuloPropEquals(
    @Nonnull String key,
    @Nonnull Object value
  ) {
    super(key, value);
  }

  @Override
  public boolean test(T t) {
    return t.getPropertyValue(getKey()) != null &&
      t.getPropertyValue(getKey()).equals(getValue());
  }

}
