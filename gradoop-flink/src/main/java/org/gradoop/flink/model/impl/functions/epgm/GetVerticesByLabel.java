package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

/**
 * Created by peet on 07.07.17.
 */
public class GetVerticesByLabel implements FlatMapFunction<GraphTransaction, Vertex> {
  private final String label;

  public GetVerticesByLabel(String label) {
    this.label = label;
  }

  @Override
  public void flatMap(GraphTransaction graph, Collector<Vertex> out) throws Exception {
    for (Vertex vertex : graph.getVertices()) {
      if (vertex.getLabel().equals(label)) {
        out.collect(vertex);
      }
    }
  }
}
