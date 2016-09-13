package org.gradoop.flink.algorithms.fsm.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Set;

public class EdgeLabels implements
  FlatMapFunction<GraphTransaction, WithCount<String>> {

  WithCount<String> reuseTuple = new WithCount<>(null, 1);

  @Override
  public void flatMap(GraphTransaction value,
    Collector<WithCount<String>> out) throws Exception {

    Set<String> edgeLabels = Sets.newHashSet();

    for (Edge edge : value.getEdges()) {
      edgeLabels.add(edge.getLabel());
    }

    for (String label : edgeLabels) {
      reuseTuple.setObject(label);
      out.collect(reuseTuple);
    }
  }
}
