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
package org.gradoop.flink.model.impl.functions.bool;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.model.impl.functions.filters.CombinableFilter;

/**
 * Logical "TRUE" as Flink function.
 *
 * @param <T> input element type
 */
public class True<T> implements MapFunction<T, Boolean>, CombinableFilter<T> {

  @Override
  public Boolean map(T t) throws Exception {
    return true;
  }

  @Override
  public boolean filter(T t) throws Exception {
    return true;
  }
}
