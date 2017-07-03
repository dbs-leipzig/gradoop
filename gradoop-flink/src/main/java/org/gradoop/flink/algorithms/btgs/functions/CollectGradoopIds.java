
package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * (a,b),(a,c) => (a,{b,c})
 * (a,{b,c}),(a,{d,e}) => (a,{b,c,d,e})
 */
public class CollectGradoopIds implements
  GroupCombineFunction
    <Tuple2<GradoopId, GradoopIdList>, Tuple2<GradoopId, GradoopIdList>>,
  GroupReduceFunction
    <Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, GradoopIdList>> {

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, GradoopId>> mappings,
    Collector<Tuple2<GradoopId, GradoopIdList>> collector) throws Exception {

    Boolean first = true;
    GradoopId vertexId = null;
    GradoopIdList btgIds = new GradoopIdList();

    for (Tuple2<GradoopId, GradoopId> pair : mappings) {
      if (first) {
        vertexId = pair.f0;
        first = false;
      }
      btgIds.add(pair.f1);
    }
    collector.collect(new Tuple2<>(vertexId, btgIds));
  }

  @Override
  public void combine(Iterable<Tuple2<GradoopId, GradoopIdList>> mappings,
    Collector<Tuple2<GradoopId, GradoopIdList>> collector) throws Exception {

    Boolean first = true;
    GradoopId vertexId = null;
    GradoopIdList btgIds = null;

    for (Tuple2<GradoopId, GradoopIdList> pair : mappings) {
      if (first) {
        vertexId = pair.f0;
        btgIds = pair.f1;
        first = false;
      }
      btgIds.addAll(pair.f1);
    }
    collector.collect(new Tuple2<>(vertexId, btgIds));
  }
}
