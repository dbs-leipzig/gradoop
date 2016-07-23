package org.gradoop.io.impl.gelly.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;

public class ToGellyEdge <E extends EPGMEdge>
  implements MapFunction<E, Edge<GradoopId, E>> {
  @Override
  public Edge<GradoopId, E> map(E e) throws Exception {
    return new Edge<>(e.getSourceId(), e.getTargetId(), e);
  }
}
