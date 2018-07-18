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

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Returns the current superstep using {@link IterationRuntimeContext}.
 *
 * Note that this function can only be applied in an iterative context (i.e.
 * bulk or delta iteration).
 *
 * @param <T> input type
 */
public class Superstep<T> extends RichMapFunction<T, Integer> {

  /**
   * super step
   */
  private Integer superstep;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    superstep = getIterationRuntimeContext().getSuperstepNumber();
  }

  @Override
  public Integer map(T value) throws Exception {
    return superstep;
  }
}
