package org.gradoop.model.impl.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;


public class ToGradoopIds
  implements GroupReduceFunction<GradoopId, GradoopIdSet> {

  @Override
  public void reduce(
    Iterable<GradoopId> iterable, Collector<GradoopIdSet> collector)
    throws
    Exception {

    GradoopIdSet ids = new GradoopIdSet();

    for(GradoopId id : iterable) {
      ids.add(id);
    }

    collector.collect(ids);

  }
}
