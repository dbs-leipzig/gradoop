/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Creates a new {@link GradoopId} for the input element and returns both.
 *
 * t -> (t, id)
 *
 * @param <T>
 */
@FunctionAnnotation.ForwardedFields("*->f0")
public class PairElementWithNewId<T>
  implements MapFunction<T, Tuple2<T, GradoopId>> {

  /**
   * Reduce object instantiations
   */
  private final Tuple2<T, GradoopId> reuseTuple = new Tuple2<>();

  @Override
  public Tuple2<T, GradoopId> map(T input) {
    reuseTuple.f0 = input;
    reuseTuple.f1 = GradoopId.get();
    return reuseTuple;
  }
}
