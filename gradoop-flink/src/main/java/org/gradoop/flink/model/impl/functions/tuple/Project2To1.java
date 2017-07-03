
package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * (f0,f1) => (f1)
 *
 * @param <T0> f0 type
 * @param <T1> f1 type
 */
@FunctionAnnotation.ForwardedFields("f1->f0")
public class Project2To1<T0, T1>
  implements MapFunction<Tuple2<T0, T1>, Tuple1<T1>> {

  /**
   * Reduce object instantiation
   */
  private final Tuple1<T1> reuseTuple = new Tuple1<>();

  @Override
  public Tuple1<T1> map(Tuple2<T0, T1> pair) throws Exception {
    reuseTuple.f0 = pair.f1;
    return reuseTuple;
  }
}
