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
import org.apache.flink.api.java.DataSet;

/**
 * Logical "NOT" as Flink function.
 */
public class Not implements MapFunction<Boolean, Boolean> {

  @Override
  public Boolean map(Boolean b) throws Exception {
    return !b;
  }

  /**
   * Map a a boolean dataset to its inverse.
   *
   * @param b boolean dataset
   * @return inverse dataset
   */
  public static DataSet<Boolean> map(DataSet<Boolean> b) {
    return b.map(new Not());
  }
}
