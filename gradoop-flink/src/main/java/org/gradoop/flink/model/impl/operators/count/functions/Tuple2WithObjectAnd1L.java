
package org.gradoop.flink.model.impl.operators.count.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * object => (object, 1)
 *
 * @param <T> value type
 */
@FunctionAnnotation.ForwardedFields("*->f0")
public class Tuple2WithObjectAnd1L<T> implements MapFunction<T, Tuple2<T, Long>> {

  /**
   * Reduce instantiations
   */
  private final Tuple2<T, Long> reuseTuple;

  /**
   * Constructor
   */
  public Tuple2WithObjectAnd1L() {
    reuseTuple = new Tuple2<>();
    reuseTuple.f1 = 1L;
  }

  @Override
  public Tuple2<T, Long> map(T t) throws Exception {
    reuseTuple.f0 = t;
    return reuseTuple;
  }
}
