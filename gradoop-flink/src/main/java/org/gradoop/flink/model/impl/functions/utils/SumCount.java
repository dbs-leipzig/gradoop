

package org.gradoop.flink.model.impl.functions.utils;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Iterator;

/**
 * (t, count1),..,(t, countN) => (t, SUM(count1,..,countN))
 *
 * @param <T> data type
 */
public class SumCount<T>
  implements GroupCombineFunction<WithCount<T>, WithCount<T>> {

  @Override
  public void combine(Iterable<WithCount<T>> iterable,
    Collector<WithCount<T>> collector) throws Exception {

    Iterator<WithCount<T>> iterator = iterable.iterator();
    WithCount<T> withCount = iterator.next();

    while (iterator.hasNext()) {
      withCount.setCount(withCount.getCount() + iterator.next().getCount());
    }

    collector.collect(withCount);
  }
}
