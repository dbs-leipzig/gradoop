package org.gradoop.model.impl.functions.counting;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * (value) => (value, 1)
 *
 * @param <T> value type
 */
public class Tuple2WithObjectAnd1L<T>
  implements MapFunction<T, Tuple2<T, Long>> {

  @Override
  public Tuple2<T, Long> map(T t) throws Exception {
    return new Tuple2<>(t, 1L);
  }
}
