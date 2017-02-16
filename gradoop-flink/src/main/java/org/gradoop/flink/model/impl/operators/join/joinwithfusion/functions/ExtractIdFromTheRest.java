package org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Groups the tuples from the first key, and performs a projection
 * @param <K> element to be extracted
 * @param <TheRest> element to be ignored
 *
 * Created by vasistas on 16/02/17.
 */
public class ExtractIdFromTheRest<K,TheRest> implements
  MapPartitionFunction<Tuple2<K,TheRest>, K> {
  @Override
  public void mapPartition(Iterable<Tuple2<K, TheRest>> values, Collector<K> out) throws Exception {
    for (Tuple2<K,TheRest> x : values) {
      out.collect(x.f0);
      break;
    }
  }
}
