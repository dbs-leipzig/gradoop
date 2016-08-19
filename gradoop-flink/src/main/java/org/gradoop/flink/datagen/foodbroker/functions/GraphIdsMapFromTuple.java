package org.gradoop.flink.datagen.foodbroker.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

import java.util.Map;

public class GraphIdsMapFromTuple implements
  GroupReduceFunction<Tuple2<GradoopId, GradoopIdSet>, Map<GradoopId, GradoopIdSet>> {

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, GradoopIdSet>> iterable,
    Collector<Map<GradoopId, GradoopIdSet>> collector)
    throws Exception {
    Map<GradoopId, GradoopIdSet> map = Maps.newHashMap();
    GradoopIdSet graphIds;
    for (Tuple2<GradoopId, GradoopIdSet> tuple : iterable) {
      if (map.containsKey(tuple.f0)) {
        graphIds = map.get(tuple.f0);
        graphIds.addAll(tuple.f1);
      } else {
        graphIds = tuple.f1;
      }
      map.put(tuple.f0, graphIds);
    }
    collector.collect(map);

  }
}
