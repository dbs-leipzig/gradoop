
package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * (f0,f1) => f0
 *
 * @param <T0> f0 type
 * @param <T1> f1 type
 */
@FunctionAnnotation.ForwardedFields("f1->*")
public class Value1Of2<T0, T1>
  implements MapFunction<Tuple2<T0, T1>, T1>, KeySelector<Tuple2<T0, T1>, T1> {

  @Override
  public T1 map(Tuple2<T0, T1> pair) throws Exception {
    return pair.f1;
  }

  @Override
  public T1 getKey(Tuple2<T0, T1> pair) throws Exception {
    return pair.f1;
  }
}
