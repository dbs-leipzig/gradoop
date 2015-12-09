package org.gradoop.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Transform an edge into a Tuple3 of edge id, source vertex and
 * target id
 *
 * @param <E> EPGM edge type
 */
public class EdgeToTupleMapper<E extends EPGMEdge> implements
  MapFunction<E, Tuple3<E, GradoopId, GradoopId>> {
  @Override
  public Tuple3<E, GradoopId, GradoopId> map(E edge) {
    return new Tuple3<>(edge, edge.getSourceId(), edge.getTargetId());
  }
}
