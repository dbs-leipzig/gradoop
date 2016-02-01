package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;

public class GellyEdgeWithoutPayload<E extends EPGMEdge> implements
  MapFunction<E, Edge<GradoopId, NullValue>> {

  @Override
  public Edge<GradoopId, NullValue> map(E e) throws Exception {
    return new Edge<>(e.getSourceId(), e.getTargetId(), new NullValue());
  }
}
