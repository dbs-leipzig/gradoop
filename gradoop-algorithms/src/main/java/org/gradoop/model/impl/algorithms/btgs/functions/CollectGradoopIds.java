package org.gradoop.model.impl.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;


public class CollectGradoopIds implements
  GroupCombineFunction
    <Tuple2<GradoopId, GradoopIdSet>, Tuple2<GradoopId, GradoopIdSet>>,
  GroupReduceFunction
    <Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, GradoopIdSet>> {


  @Override
  public void reduce(Iterable<Tuple2<GradoopId, GradoopId>> mappings,
    Collector<Tuple2<GradoopId, GradoopIdSet>> collector) throws Exception {

    Boolean first = true;
    GradoopId vertexId = null;
    GradoopIdSet btgIds = new GradoopIdSet();

    for(Tuple2<GradoopId, GradoopId> pair : mappings) {

      if(first) {
        vertexId = pair.f0;
        first = false;
      }

      btgIds.add(pair.f1);
    }

    collector.collect(new Tuple2<>(vertexId, btgIds));
  }

  @Override
  public void combine(Iterable<Tuple2<GradoopId, GradoopIdSet>> mappings,
    Collector<Tuple2<GradoopId, GradoopIdSet>> collector) throws Exception {

    Boolean first = true;
    GradoopId vertexId = null;
    GradoopIdSet btgIds = null;

    for(Tuple2<GradoopId, GradoopIdSet> pair : mappings) {

      if(first) {
        vertexId = pair.f0;
        btgIds = pair.f1;
        first = false;
      }

      btgIds.addAll(pair.f1);
    }

    collector.collect(new Tuple2<>(vertexId, btgIds));

  }


}
