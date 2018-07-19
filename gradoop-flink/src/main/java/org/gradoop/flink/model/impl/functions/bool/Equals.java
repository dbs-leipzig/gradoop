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

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.DataSet;

/**
 * Equality as Flink function.
 *
 * @param <T> input element type
 */
public class Equals<T>
  implements CrossFunction<T, T, Boolean> {

  @Override
  public Boolean cross(T left, T right) throws Exception {
    return left.equals(right);
  }

  /**
   * Checks for pair-wise equality between the elements of the given input sets.
   *
   * @param first   first input dataset
   * @param second  second input dataset
   * @param <T>     dataset element type
   * @return dataset with {@code boolean} values for each pair
   */
  public static <T> DataSet<Boolean> cross(
    DataSet<T> first, DataSet<T> second) {
    return first.cross(second).with(new Equals<T>());
  }
}
