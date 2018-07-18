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
package org.gradoop.flink.model.impl.operators.count.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Maps something to numeric ONE in a tuple 1.
 *
 * @param <T> type of something
 */
public class Tuple1With1L<T>
  implements JoinFunction<T, T, Tuple1<Long>>, MapFunction<T, Tuple1<Long>> {

  /**
   * Numeric one
   */
  private static final Tuple1<Long> ONE = new Tuple1<>(1L);

  @Override
  public Tuple1<Long> join(T left, T right) throws Exception {
    return ONE;
  }

  @Override
  public Tuple1<Long> map(T x) throws Exception {
    return ONE;
  }
}

