package org.gradoop.flink.algorithms.jaccardindex.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * COPIED WITH SMALL MODIFICATIONS FROM
 * {@link org.apache.flink.graph.library.similarity.JaccardIndex}
 * This is the first of three operations implementing a self-join to generate
 * the full neighbor pairing for each vertex. The number of neighbor pairs
 * is (n choose 2) which is quadratic in the vertex degree.
 * <p>
 * The third operation, {@link GenerateGroupPairs}, processes groups of size
 * {@link #groupSize} and emits {@code O(groupSize * deg(vertex))} pairs.
 * <p>
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
  private final int groupSize;

  private IntValue groupSpansValue = new IntValue();

  private Tuple4<IntValue, GradoopId, GradoopId, IntValue> output =
    new Tuple4<>(groupSpansValue, null, null, new IntValue());

  public GenerateGroupSpans(int groupSize) {
    this.groupSize = groupSize;
  }

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

      if (++groupCount == groupSize) {
        groupCount = 0;
        groupSpansValue.setValue(++groupSpans);
      }
    }
  }
}