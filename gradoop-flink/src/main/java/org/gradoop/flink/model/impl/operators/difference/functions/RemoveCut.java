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
package org.gradoop.flink.model.impl.operators.difference.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * If a group only contains one element, return it. Else return nothing.
 *
 * @param <O> any object type
 */
public class RemoveCut<O>
  implements GroupReduceFunction<Tuple2<O, Long>, O> {

  @Override
  public void reduce(Iterable<Tuple2<O, Long>> iterable,
    Collector<O> collector) throws Exception {
    boolean inFirst = false;
    boolean inSecond = false;

    O o = null;

    for (Tuple2<O, Long> tuple : iterable) {
      o = tuple.f0;
      if (tuple.f1 == 1L) {
        inFirst = true;
      } else {
        inSecond = true;
      }
    }
    if (inFirst && !inSecond) {
      collector.collect(o);
    }
  }
}
