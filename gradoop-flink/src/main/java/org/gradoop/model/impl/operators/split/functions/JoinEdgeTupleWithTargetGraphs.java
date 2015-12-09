package org.gradoop.model.impl.operators.split.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;

import java.util.List;

/**
 * Join edge tuples with the graph sets of their targets
 *
 * @param <E> EPGM edge type
 */
public class JoinEdgeTupleWithTargetGraphs<E extends EPGMEdge> implements
  JoinFunction<Tuple3<E, List<GradoopId>, GradoopId>, Tuple2<GradoopId,
  List<GradoopId>>, Tuple3<E, List<GradoopId>, List<GradoopId>>> {
  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple3<E, List<GradoopId>, List<GradoopId>> join(
    Tuple3<E, List<GradoopId>, GradoopId> tuple3, Tuple2<GradoopId,
    List<GradoopId>> tuple2) throws
    Exception {
    return new Tuple3<>(tuple3.f0, tuple3.f1,
      (List<GradoopId>) Lists.newArrayList(tuple2.f1));
  }
}
