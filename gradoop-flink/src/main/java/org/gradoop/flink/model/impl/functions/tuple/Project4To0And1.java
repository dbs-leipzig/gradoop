
package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * (f0,f1,f2,f3) => (f0,f1)
 *
 * @param <T0> f0 type
 * @param <T1> f1 type
 * @param <T2> f2 type
 * @param <T3> f3 type
 */
@FunctionAnnotation.ReadFields("f0;f1")
@FunctionAnnotation.ForwardedFields("f0->f0;f1->f1")
public class Project4To0And1<T0, T1, T2, T3>
  implements MapFunction<Tuple4<T0, T1, T2, T3>, Tuple2<T0, T1>> {

  /**
   * Reduce instantiations
   */
  private final Tuple2<T0, T1> reuseTuple = new Tuple2<>();

  @Override
  public Tuple2<T0, T1> map(Tuple4<T0, T1, T2, T3> quad) throws Exception {
    reuseTuple.setFields(quad.f0, quad.f1);
    return reuseTuple;
  }
}
