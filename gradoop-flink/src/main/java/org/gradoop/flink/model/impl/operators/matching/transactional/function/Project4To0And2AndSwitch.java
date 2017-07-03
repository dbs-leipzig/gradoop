
package org.gradoop.flink.model.impl.operators.matching.transactional.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * (f0,f1,f2,f3) => (f2,f0)
 *
 * @param <T0> f0 type
 * @param <T1> f1 type
 * @param <T2> f2 type
 * @param <T3> f3 type
 */
@FunctionAnnotation.ForwardedFields("f0->f1,f2->f0")
public class Project4To0And2AndSwitch<T0, T1, T2, T3>
  implements MapFunction<Tuple4<T0, T1, T2, T3>, Tuple2<T2, T0>> {

  /**
   * Reduce instantiations
   */
  private final Tuple2<T2, T0> reuseTuple = new Tuple2<>();

  @Override
  public Tuple2<T2, T0> map(Tuple4<T0, T1, T2, T3> triple) throws Exception {
    reuseTuple.setFields(triple.f2, triple.f0);
    return reuseTuple;
  }
}
