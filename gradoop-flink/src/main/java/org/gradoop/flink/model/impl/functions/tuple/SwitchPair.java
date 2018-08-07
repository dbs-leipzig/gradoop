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
package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Switches the fields of a Flink pair
 * @param <A> type of field 0
 * @param <B> type of field 1
 */
@FunctionAnnotation.ForwardedFields("f0->f1;f1->f0")
public class SwitchPair<A, B>
  implements MapFunction<Tuple2<A, B>, Tuple2<B, A>> {

  /**
   * Reduce object instantiations.
   */
  private final Tuple2<B, A> reuse = new Tuple2<>();

  @Override
  public Tuple2<B, A> map(Tuple2<A, B> pair) throws Exception {
    reuse.f0 = pair.f1;
    reuse.f1 = pair.f0;
    return reuse;
  }
}
