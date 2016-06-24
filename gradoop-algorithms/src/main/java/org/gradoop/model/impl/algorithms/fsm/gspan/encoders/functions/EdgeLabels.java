package org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.tuples
  .EdgeTripleWithStringEdgeLabel;

import java.util.Collection;
import java.util.Set;


public class EdgeLabels implements
  FlatMapFunction<Collection<EdgeTripleWithStringEdgeLabel>, String> {


  @Override
  public void flatMap(
    Collection<EdgeTripleWithStringEdgeLabel> edges,
    Collector<String> collector) throws Exception {

    Set<String> labels = Sets.newHashSet();

    for (EdgeTripleWithStringEdgeLabel edge : edges) {
      labels.add(edge.getEdgeLabel());
    }

    for (String label : labels) {
      collector.collect(label);
    }

  }
}
