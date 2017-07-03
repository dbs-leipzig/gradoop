
package org.gradoop.flink.algorithms.fsm.dimspan.functions.preprocessing;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphStringString;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Set;

/**
 * graph -> (vertexLabel,1L),..
 */
public class ReportVertexLabels implements FlatMapFunction<LabeledGraphStringString, WithCount<String>> {

  /**
   * reuse tuple to avoid instantiations
   */
  private WithCount<String> reuseTuple = new WithCount<>(null, 1);

  @Override
  public void flatMap(LabeledGraphStringString graph,
    Collector<WithCount<String>> out) throws Exception {

    Set<String> vertexLabels = Sets.newHashSet(graph.getVertexLabels());

    for (String label : vertexLabels) {
      reuseTuple.setObject(label);
      out.collect(reuseTuple);
    }
  }
}
