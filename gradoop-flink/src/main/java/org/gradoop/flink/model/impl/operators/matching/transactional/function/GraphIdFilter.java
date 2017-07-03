
package org.gradoop.flink.model.impl.operators.matching.transactional.function;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.List;

/**
 * Filters a set of Tuple2 with GradoopIds in the first field by the
 * containment of this id in a broadcast set.
 *
 * @param <T> any type
 */
public class GraphIdFilter<T> extends RichFilterFunction<Tuple2<GradoopId, T>> {

  /**
   * Broadcast set of gradoop ids
   */
  private List<GradoopId> graphIds;

  @Override
  public void open(Configuration parameters) throws Exception {
    this.graphIds = getRuntimeContext().getBroadcastVariable("graph-ids");
  }

  @Override
  public boolean filter(Tuple2<GradoopId, T> tuple2) throws Exception {
    return graphIds.contains(tuple2.f0);
  }
}
