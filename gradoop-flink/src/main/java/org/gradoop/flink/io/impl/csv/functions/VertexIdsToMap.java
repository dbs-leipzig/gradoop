package org.gradoop.flink.io.impl.csv.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Map;

/**
 * Created by stephan on 14.10.16.
 */
public class VertexIdsToMap implements GroupReduceFunction<Tuple2<String,
  GradoopId>, Map<String, GradoopId>> {

  @Override
  public void reduce(Iterable<Tuple2<String, GradoopId>> iterable,
    Collector<Map<String, GradoopId>> collector) throws Exception {

    Map<String, GradoopId> map = Maps.newHashMap();
    for (Tuple2<String, GradoopId> tuple : iterable) {
      map.put(tuple.f0, tuple.f1);
    }
    collector.collect(map);
  }
}
