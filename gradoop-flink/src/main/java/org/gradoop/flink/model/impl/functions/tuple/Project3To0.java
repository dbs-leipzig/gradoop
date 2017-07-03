
package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * (f0,f1,f2) => (f0)
 *
 * @param <T0> f0 type
 * @param <T1> f1 type
 * @param <T2> f2 type
 */
@FunctionAnnotation.ReadFields("f0")
@FunctionAnnotation.ForwardedFields("f0")
public class Project3To0<T0, T1, T2> implements MapFunction<Tuple3<T0, T1, T2>, Tuple1<T0>> {

  /**
   * Reduce instantiations
   */
  private final Tuple1<T0> reuseTuple = new Tuple1<>();

  @Override
  public Tuple1<T0> map(Tuple3<T0, T1, T2> triple) throws Exception {
    reuseTuple.setFields(triple.f0);
    return reuseTuple;
  }
}
