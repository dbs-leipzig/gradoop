package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * Created by vasistas on 05/04/17.
 */
public class ExtendTupleWithGraphList<K extends Comparable<K>>
  implements GroupCombineFunction<Tuple2<ImportEdge<K>, GradoopId>,
                                  Tuple2<ImportEdge<K>, GradoopIdList>> {

  private final Tuple2<ImportEdge<K>, GradoopIdList> reusable;

  public ExtendTupleWithGraphList() {
    reusable = new Tuple2<>();
  }

  @Override
  public void combine(Iterable<Tuple2<ImportEdge<K>, GradoopId>> iterable,
    Collector<Tuple2<ImportEdge<K>, GradoopIdList>> collector) throws Exception {
    Tuple2<ImportEdge<K>, GradoopId> first = null;
    reusable.f1.clear();
    for (Tuple2<ImportEdge<K>, GradoopId> x : iterable) {
      if (first == null) {
        reusable.f0 = x.f0;
        first = x;
      }
      reusable.f1.add(x.f1);
    }
    collector.collect(reusable);
  }
}
