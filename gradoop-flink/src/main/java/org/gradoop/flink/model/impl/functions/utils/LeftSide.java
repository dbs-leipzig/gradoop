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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;

/**
 * left, right => left
 *
 * @param <L> left type
 * @param <R> right type
 */
@FunctionAnnotation.ForwardedFieldsFirst("*->*")
public class LeftSide<L, R>
  implements CrossFunction<L, R, L>, JoinFunction<L, R, L>, CoGroupFunction<L, R, L> {

  @Override
  public L cross(L left, R right) throws Exception {
    return left;
  }

  @Override
  public L join(L first, R second) throws Exception {
    return first;
  }

  @Override
  public void coGroup(Iterable<L> first, Iterable<R> second, Collector<L> out) throws Exception {
    if (second.iterator().hasNext()) {
      for (L x : first) {
        out.collect(x);
      }
    }
  }
}
