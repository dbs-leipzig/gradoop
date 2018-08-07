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
package org.gradoop.flink.model.impl.functions.filters;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * A filter function that can be combined using a logical {@code and},
 * {@code or} and {@code not}.
 *
 * @param <T> The type of elements to filter.
 */
public interface CombinableFilter<T> extends FilterFunction<T> {

  /**
   * Combine this filter with another filter using a logical {@code and}.
   * Either filter may be a
   * {@link org.apache.flink.api.common.functions.RichFunction}.
   *
   * @param other The filter that will be combined with this filter.
   * @return The combined filter.
   */
  default CombinableFilter<T> and(FilterFunction<? super T> other) {
    return new And<>(this, other);
  }

  /**
   * Combine this filter with another filter using a logical {@code or}.
   * Either filter may be a
   * {@link org.apache.flink.api.common.functions.RichFunction}.
   *
   * @param other The filter that will be combined with this filter.
   * @return The combined filter.
   */
  default CombinableFilter<T> or(FilterFunction<? super T> other) {
    return new Or<>(this, other);
  }

  /**
   * Invert this filter, i.e. get the logical {@code not} of this filter.
   * This filter may be a
   * {@link org.apache.flink.api.common.functions.RichFunction}.
   *
   * @return The inverted filter.
   */
  default CombinableFilter<T> negate() {
    return new Not<>(this);
  }
}
