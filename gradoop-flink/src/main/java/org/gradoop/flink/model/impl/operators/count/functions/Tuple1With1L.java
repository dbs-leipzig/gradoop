
package org.gradoop.flink.model.impl.operators.count.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Maps something to numeric ONE in a tuple 1.
 *
 * @param <T> type of something
 */
public class Tuple1With1L<T>
  implements JoinFunction<T, T, Tuple1<Long>>, MapFunction<T, Tuple1<Long>> {

  /**
   * Numeric one
   */
  private static final Tuple1<Long> ONE = new Tuple1<>(1L);

  @Override
  public Tuple1<Long> join(T left, T right) throws Exception {
    return ONE;
  }

  @Override
  public Tuple1<Long> map(T x) throws Exception {
    return ONE;
  }
}

