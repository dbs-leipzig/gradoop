package org.gradoop.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * (x) => x
 *
 * @param <T> value type
 */
public class ValueOfTuple1<T> implements MapFunction<Tuple1<T>, T> {
  @Override
  public T map(Tuple1<T> tuple) throws Exception {
    return tuple.f0;
  }
}
