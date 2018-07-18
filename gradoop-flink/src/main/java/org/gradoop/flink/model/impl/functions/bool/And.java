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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;

/**
 * Logical "AND" as Flink function.
 */
public class And implements CrossFunction<Boolean, Boolean, Boolean>,
  ReduceFunction<Boolean> {

  @Override
  public Boolean cross(Boolean a, Boolean b) throws Exception {
    return a && b;
  }

  @Override
  public Boolean reduce(Boolean a, Boolean b) throws Exception {
    return a && b;
  }

  /**
   * Performs a logical conjunction on the union of both input data sets.
   *
   * @param a boolean vector a
   * @param b boolean vector b
   * @return 1-element dataset containing the result of the conjunction
   */
  public static DataSet<Boolean> union(DataSet<Boolean> a, DataSet<Boolean> b) {
    return a.union(b).reduce(new And());
  }

  /**
   * Performs a pair-wise logical conjunction on the cross of both input data
   * sets.
   *
   * @param a boolean vector a
   * @param b boolean vector b
   * @return dataset containing the result of the pair-wise conjunction
   */
  public static DataSet<Boolean> cross(DataSet<Boolean> a, DataSet<Boolean> b) {
    return a.cross(b).with(new And());
  }

  /**
   * Performs a logical conjunction on a data set of boolean values.
   *
   * @param d boolean set
   * @return conjunction
   */
  public static DataSet<Boolean> reduce(DataSet<Boolean> d) {
    return d.reduce(new And());
  }
}
