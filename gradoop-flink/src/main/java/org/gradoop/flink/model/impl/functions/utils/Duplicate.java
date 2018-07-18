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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Duplicates a data set element k times.
 *
 * @param <T> element type
 */
public class Duplicate<T> implements FlatMapFunction<T, T> {

  /**
   * number of duplicates per input element (k)
   */
  private final int multiplicand;

  /**
   * Constructor.
   *
   * @param multiplicand number of duplicates per input element
   */
  public Duplicate(int multiplicand) {
    this.multiplicand = multiplicand;
  }

  @Override
  public void flatMap(T original, Collector<T> duplicates) throws  Exception {
    for (int i = 1; i <= multiplicand; i++) {
      duplicates.collect(original);
    }
  }
}
