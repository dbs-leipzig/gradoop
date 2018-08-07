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
package org.gradoop.flink.model.impl.functions.utils;

import org.gradoop.flink.model.impl.functions.filters.CombinableFilter;

/**
 * Checks if a given object of type {@link IN} is instance of a specific class
 * of type {@link T}.
 *
 * @param <IN>  input type
 * @param <T>   class type to check
 */
public class IsInstance<IN, T> implements CombinableFilter<IN> {
  /**
   * Class for isInstance check
   */
  private final Class<T> clazz;

  /**
   * Constructor
   *
   * @param clazz class for isInstance check
   */
  public IsInstance(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public boolean filter(IN value) throws Exception {
    return clazz.isInstance(value);
  }
}
