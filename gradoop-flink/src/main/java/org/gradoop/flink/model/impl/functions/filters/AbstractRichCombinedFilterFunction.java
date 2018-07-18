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
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.util.Objects;

/**
 * A {@link RichFilterFunction} that can be combined like a
 * {@link CombinableFilter}. The {@link RuntimeContext context} will be
 * handled for each filter that is part of this combined filter.
 *
 * @param <T> The type of elements to filter.
 */
public abstract class AbstractRichCombinedFilterFunction<T>
  extends RichFilterFunction<T> implements CombinableFilter<T> {

  /**
   * The filters this filter is composed of.
   */
  protected final FilterFunction<? super T>[] components;

  /**
   * Constructor setting the component filter functions that this filter is
   * composed of.
   *
   * @param filters The filters.
   */
  @SafeVarargs
  public AbstractRichCombinedFilterFunction(
  FilterFunction<? super T>... filters) {
    for (FilterFunction<? super T> filter : filters) {
      Objects.requireNonNull(filter);
    }
    components = filters;
  }

  @Override
  public void setRuntimeContext(RuntimeContext t) {
    super.setRuntimeContext(t);
    for (FilterFunction<? super T> component : components) {
      if (component instanceof RichFunction) {
        ((RichFunction) component).setRuntimeContext(t);
      }
    }
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    for (FilterFunction<? super T> component : components) {
      if (component instanceof RichFunction) {
        ((RichFunction) component).open(parameters);
      }
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    for (FilterFunction<? super T> component : components) {
      if (component instanceof RichFunction) {
        ((RichFunction) component).close();
      }
    }
  }
}
