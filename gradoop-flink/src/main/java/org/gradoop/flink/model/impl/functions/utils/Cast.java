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

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Casts an object of type {@link IN} to type {@link T}.
 *
 * @param <IN>  input type
 * @param <T>   type to cast to
 */
public class Cast<IN, T> implements MapFunction<IN, T> {

  /**
   * Class for type cast
   */
  private final Class<T> clazz;

  /**
   * Constructor
   *
   * @param clazz class for type cast
   */
  public Cast(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public T map(IN value) throws Exception {
    return clazz.cast(value);
  }
}
