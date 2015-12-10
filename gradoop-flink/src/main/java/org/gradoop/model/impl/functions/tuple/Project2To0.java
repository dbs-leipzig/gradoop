package org.gradoop.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * (f0,f1) => (f0)
 *
 * @param <T0> f0 type
 * @param <T1> f1 type
 */
@FunctionAnnotation.ForwardedFields("f0")
public class Project2To0<T0, T1>
  implements MapFunction<Tuple2<T0, T1>, Tuple1<T0>> {

  @Override
  public Tuple1<T0> map(Tuple2<T0, T1> pair) throws Exception {
    return new Tuple1<>(pair.f0);
  }
}
