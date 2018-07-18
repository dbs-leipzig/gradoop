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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * (object) -> (object, 1L)
 *
 * @param <T> type of object
 */
public class Tuple2FromTupleWithObjectAnd1L<T> implements MapFunction<Tuple1<T>, Tuple2<T, Long>> {
  /**
   * Reduce object instantiations
   */
  private final Tuple2<T, Long> reuseTuple;

  /**
   * Constructor
   */
  public Tuple2FromTupleWithObjectAnd1L() {
    reuseTuple = new Tuple2<>();
    reuseTuple.f1 = 1L;
  }

  @Override
  public Tuple2<T, Long> map(Tuple1<T> value) throws Exception {
    reuseTuple.f0 = value.f0;
    return reuseTuple;
  }
}
