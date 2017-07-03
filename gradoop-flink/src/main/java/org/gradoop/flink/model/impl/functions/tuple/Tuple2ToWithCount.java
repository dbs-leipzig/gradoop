
package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * (object, count) -> WithCount(object, count)
 *
 * @param <T> object type
 */
@FunctionAnnotation.ForwardedFields("f0;f1")
public class Tuple2ToWithCount<T> implements MapFunction<Tuple2<T, Long>, WithCount<T>> {
  /**
   * Reduce object instantiations
   */
  private final WithCount<T> reuseTuple = new WithCount<>();

  @Override
  public WithCount<T> map(Tuple2<T, Long> value) throws Exception {
    reuseTuple.setObject(value.f0);
    reuseTuple.setCount(value.f1);
    return reuseTuple;
  }
}
