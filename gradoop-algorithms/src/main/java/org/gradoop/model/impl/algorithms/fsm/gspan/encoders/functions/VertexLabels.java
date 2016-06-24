package org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.tuples.GraphTransaction;

import java.util.Set;


public class VertexLabels
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements FlatMapFunction<GraphTransaction<G, V, E>, String> {

  @Override
  public void flatMap(GraphTransaction<G, V, E> transaction,
    Collector<String> collector) throws Exception {

    Set<String> labels = Sets.newHashSet();

    for (V vertex : transaction.getVertices()) {
      labels.add(vertex.getLabel());
    }

    for (String label : labels) {
      collector.collect(label);
    }

  }
}
