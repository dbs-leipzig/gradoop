package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Created by vasistas on 08/04/17.
 */
public class AggregateTheSameEdgeWithinDifferentGraphs<K extends Comparable<K>> implements
  GroupCombineFunction<Tuple6<K, GradoopId, K, String, Properties, GradoopId>,
    Tuple6<K, GradoopId, K, String, Properties, GradoopIdList>> {

  private final Tuple6<K, GradoopId, K, String, Properties, GradoopIdList> reusable;

  public AggregateTheSameEdgeWithinDifferentGraphs() {
    reusable = new Tuple6<>();
    reusable.f5 = new GradoopIdList();
  }

  @Override
  public void combine(
    Iterable<Tuple6<K, GradoopId, K, String, Properties, GradoopId>> values,
    Collector<Tuple6<K, GradoopId, K, String, Properties, GradoopIdList>> out) throws
    Exception {
    reusable.f5.clear();
    K key = null;
    for (Tuple6<K, GradoopId, K, String, Properties, GradoopId> x : values) {
      if (key == null) {
        key = x.f0;
      }
      reusable.f0 = x.f0;
      reusable.f1 = x.f1;
      reusable.f2 = x.f2;
      reusable.f3 = x.f3;
      reusable.f4 = x.f4;
      if (!reusable.f5.contains(x.f5)) {
        reusable.f5.add(x.f5);
      }
    }
    out.collect(reusable);
  }
}
