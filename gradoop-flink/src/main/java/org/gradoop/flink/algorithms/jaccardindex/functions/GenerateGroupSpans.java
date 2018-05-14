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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

import static org.apache.flink.graph.library.similarity.JaccardIndex.DEFAULT_GROUP_SIZE;

/**
 * COPIED WITH SOME MODIFICATIONS FROM
 * {@link org.apache.flink.graph.library.similarity.JaccardIndex}
 * This is the first of three operations implementing a self-join to generate
 * the full neighbor pairing for each vertex. The number of neighbor pairs
 * is (n choose 2) which is quadratic in the vertex degree.
 *
 * The third operation, {@link GenerateGroupPairs}, processes groups of size 64 and emits
 * {@code O(groupSize * deg(vertex))} pairs.
 *
 * This input to the third operation is still quadratic in the vertex degree.
 * Two prior operations, {@link GenerateGroupSpans} and {@link GenerateGroups},
 * each emit datasets linear in the vertex degree, with a forced rebalance
 * in between. {@link GenerateGroupSpans} first annotates each edge with the
 * number of groups and {@link GenerateGroups} emits each edge into each group.
 */
@FunctionAnnotation.ForwardedFields("0->1; 1->2")
public class GenerateGroupSpans implements
  GroupReduceFunction<Tuple3<GradoopId, GradoopId, Long>, Tuple4<IntValue, GradoopId, GradoopId,
    IntValue>> {

  /**
   * GroupSpan counter
   */
  private IntValue groupSpansValue = new IntValue();

  /**
   * Output Tuple
   */
  private Tuple4<IntValue, GradoopId, GradoopId, IntValue> output =
    new Tuple4<>(groupSpansValue, null, null, new IntValue());

  @Override
  public void reduce(Iterable<Tuple3<GradoopId, GradoopId, Long>> values,
    Collector<Tuple4<IntValue, GradoopId, GradoopId, IntValue>> out) throws Exception {
    int groupCount = 0;
    int groupSpans = 1;

    groupSpansValue.setValue(groupSpans);

    for (Tuple3<GradoopId, GradoopId, Long> edge : values) {
      long degree = edge.f2;
      if (degree > Integer.MAX_VALUE) {
        throw new RuntimeException("Degree overflows IntValue");
      }

      // group span, u, v, d(v)
      output.f1 = edge.f0;
      output.f2 = edge.f1;
      output.f3.setValue((int) degree);

      out.collect(output);

      if (++groupCount == DEFAULT_GROUP_SIZE) {
        groupCount = 0;
        groupSpansValue.setValue(++groupSpans);
      }
    }
  }
}
