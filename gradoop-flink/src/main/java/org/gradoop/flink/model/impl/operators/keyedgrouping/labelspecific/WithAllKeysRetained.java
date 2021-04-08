/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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

import org.gradoop.flink.model.api.functions.KeyFunction;
import org.gradoop.flink.model.impl.functions.filters.CombinableFilter;

import java.util.List;
import java.util.Objects;

/**
 * A filter function selecting elements with all values set to a default value.
 *
 * @param <E> The type of the elements to filter.
 */
public class WithAllKeysRetained<E> implements CombinableFilter<E> {

  /**
   * The keys to check on each element.
   */
  private final List<KeyFunction<E, ?>> keys;

  /**
   * Create a new instance of this filter function.
   *
   * @param keys The list of key functions to check.
   */
  public WithAllKeysRetained(List<KeyFunction<E, ?>> keys) {
    this.keys = Objects.requireNonNull(keys);
  }

  @Override
  public boolean filter(E value) {
    return keys.stream().allMatch(k -> k.retainElement(value));
  }
}
