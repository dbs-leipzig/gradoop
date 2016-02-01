package org.gradoop.model.impl.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;

public class BtgVertex<V extends EPGMVertex> implements
  MapFunction<V, Vertex<GradoopId, GradoopId>> {

  @Override
  public Vertex<GradoopId, GradoopId> map(V v) throws Exception {
    GradoopId id = v.getId();
    return new Vertex<>(id, id);
  }
}
