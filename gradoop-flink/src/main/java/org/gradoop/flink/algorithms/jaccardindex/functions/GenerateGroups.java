package org.gradoop.flink.algorithms.jaccardindex.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * COPIED WITH SMALL MODIFICATIONS FROM
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
