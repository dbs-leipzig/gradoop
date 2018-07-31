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
 * A filter that combines multiple filters using a logical {@code and}.
 * This filter will therefore accept all objects that are accepted by
 * every component filter.
 *
 * @param <T> The type of objects to filter.
 */
public class And<T> extends AbstractRichCombinedFilterFunction<T> {

  /**
   * Constructor setting the filter to be combined with a logical {@code and}.
   *
   * @param filters The filters.
   */
  @SafeVarargs
  public And(FilterFunction<? super T>... filters) {
    super(filters);
  }

  @Override
  public boolean filter(T value) throws Exception {
    for (FilterFunction<? super T> componentFilter : components) {
      if (!componentFilter.filter(value)) {
        return false;
      }
    }
    return true;
  }
}
