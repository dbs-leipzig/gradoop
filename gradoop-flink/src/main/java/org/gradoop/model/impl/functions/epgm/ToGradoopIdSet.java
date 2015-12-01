package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

/**
 * id1,..,idn => {id1,..,idn}
 */
public class ToGradoopIdSet
  implements GroupReduceFunction<GradoopId, GradoopIdSet> {

  @Override
  public void reduce(Iterable<GradoopId> iterable,
    Collector<GradoopIdSet> collector) throws Exception {

    GradoopIdSet ids = new GradoopIdSet();

    for (GradoopId id : iterable) {
      ids.add(id);
    }

    collector.collect(ids);
  }
}
