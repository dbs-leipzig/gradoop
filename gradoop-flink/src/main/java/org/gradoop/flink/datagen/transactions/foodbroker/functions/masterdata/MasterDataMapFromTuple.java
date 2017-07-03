
package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Map;

/**
 * Returns a map from each gradoop id to the object.
 *
 * @param <T> Type of the maps value
 */
public class MasterDataMapFromTuple<T>
  implements GroupReduceFunction<Tuple2<GradoopId, T>, Map<GradoopId, T>> {

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, T>> iterable,
    Collector<Map<GradoopId, T>> collector) throws Exception {
    Map<GradoopId, T> map = Maps.newHashMap();
    // fill map with all tuple pairs
    for (Tuple2<GradoopId, T> tuple : iterable) {
      map.put(tuple.f0, tuple.f1);
    }
    collector.collect(map);
  }
}
