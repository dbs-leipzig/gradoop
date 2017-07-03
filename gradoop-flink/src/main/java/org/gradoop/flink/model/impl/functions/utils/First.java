
package org.gradoop.flink.model.impl.functions.utils;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

/**
 * t1,t2,..,tn => t1
 * @param <T> data type
 */
public class First<T> implements GroupReduceFunction<T, T> {

  @Override
  public void reduce(Iterable<T> iterable, Collector<T> collector) throws Exception {
    collector.collect(iterable.iterator().next());
  }
}
