package org.gradoop.flink.model.impl.operators.fusion.edgereduce;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.fusion.edgereduce.tuples.VertexIdToEdgeId;

/**
 * Created by vasistas on 23/02/17.
 */
public class FlatMapEdgesToVertices implements FlatMapFunction<Edge,
    VertexIdToEdgeId> {
  private final VertexIdToEdgeId r = new VertexIdToEdgeId();
  @Override
  public void flatMap(Edge value, Collector<VertexIdToEdgeId> out) throws Exception {
    r.f0 = value.getSourceId();
    r.f1 = value.getId();
    out.collect(r);
    r.f0 = value.getTargetId();
    out.collect(r);
  }
}
