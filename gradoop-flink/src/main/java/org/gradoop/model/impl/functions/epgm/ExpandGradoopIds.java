package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

/**
 * Created by peet on 02.02.16.
 */
public class ExpandGradoopIds implements FlatMapFunction
  <Tuple2<GradoopId, GradoopIdSet>, Tuple2<GradoopId, GradoopId>> {

  @Override
  public void flatMap(
    Tuple2<GradoopId, GradoopIdSet> pair,
    Collector<Tuple2<GradoopId, GradoopId>> collector) throws Exception {

    GradoopId fromId = pair.f0;

    for(GradoopId toId : pair.f1) {
      collector.collect(new Tuple2<>(fromId, toId));
    }

  }
}
