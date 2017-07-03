
package org.gradoop.flink.model.impl.operators.difference.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * If a group only contains one element, return it. Else return nothing.
 *
 * @param <O> any object type
 */
public class RemoveCut<O>
  implements GroupReduceFunction<Tuple2<O, Long>, O> {

  @Override
  public void reduce(Iterable<Tuple2<O, Long>> iterable,
    Collector<O> collector) throws Exception {
    boolean inFirst = false;
    boolean inSecond = false;

    O o = null;

    for (Tuple2<O, Long> tuple : iterable) {
      o = tuple.f0;
      if (tuple.f1 == 1L) {
        inFirst = true;
      } else {
        inSecond = true;
      }
    }
    if (inFirst && !inSecond) {
      collector.collect(o);
    }
  }
}
