
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * id1,..,idn => {id1,..,idn}
 */
public class ToGradoopIdSet
  implements GroupReduceFunction<GradoopId, GradoopIdList> {

  @Override
  public void reduce(Iterable<GradoopId> iterable,
    Collector<GradoopIdList> collector) throws Exception {

    GradoopIdList ids = new GradoopIdList();

    for (GradoopId id : iterable) {
      ids.add(id);
    }

    collector.collect(ids);
  }
}
