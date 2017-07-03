
package org.gradoop.flink.model.impl.operators.count.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * (object) -> (object, 1L)
 *
 * @param <T> type of object
 */
public class Tuple2FromTupleWithObjectAnd1L<T> implements MapFunction<Tuple1<T>, Tuple2<T, Long>> {
  /**
   * Reduce object instantiations
   */
  private final Tuple2<T, Long> reuseTuple;

  /**
   * Constructor
   */
  public Tuple2FromTupleWithObjectAnd1L() {
    reuseTuple = new Tuple2<>();
    reuseTuple.f1 = 1L;
  }

  @Override
  public Tuple2<T, Long> map(Tuple1<T> value) throws Exception {
    reuseTuple.f0 = value.f0;
    return reuseTuple;
  }
}
