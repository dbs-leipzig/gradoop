/**
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

package org.gradoop.flink.algorithms.jaccardindex.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * COPIED WITH SOME MODIFICATIONS FROM
 * {@link org.apache.flink.graph.library.similarity.JaccardIndex}
 * Emits the input tuple into each group within its group span.
 *
 * @see GenerateGroupSpans
 */
@FunctionAnnotation.ForwardedFields("1; 2; 3")
public class GenerateGroups implements
  FlatMapFunction<Tuple4<IntValue, GradoopId, GradoopId, IntValue>, Tuple4<IntValue, GradoopId,
    GradoopId, IntValue>> {
  @Override
  public void flatMap(Tuple4<IntValue, GradoopId, GradoopId, IntValue> value,
    Collector<Tuple4<IntValue, GradoopId, GradoopId, IntValue>> out) throws Exception {
    int spans = value.f0.getValue();

    for (int idx = 0; idx < spans; idx++) {
      value.f0.setValue(idx);
      out.collect(value);
    }
  }
}
