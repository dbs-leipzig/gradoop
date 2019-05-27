package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;

/**
 * Only collects a value, if it is not part of the second dataset.
 * {@code Result = A - B}

 * @param <T>
 */
public class SubtractCoGroupFunction<T> implements CoGroupFunction<T, T, T> {

  @Override
  public void coGroup(Iterable<T> first, Iterable<T> second, Collector<T> out) throws Exception {
    // only collect first that does not exist second
    if (!second.iterator().hasNext()) {
      first.forEach(out::collect);
    }
  }
}
