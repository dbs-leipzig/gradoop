package org.gradoop.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * object -> (object)
 */
@FunctionAnnotation.ForwardedFields("*->f0")
public class ObjectTo1<T> implements MapFunction<T, Tuple1<T>> {
  /**
   * Reduce instantiations.
   */
  private final Tuple1<T> reuseTuple = new Tuple1<>();

  @Override
  public Tuple1<T> map(T value) throws Exception {
    reuseTuple.f0 = value;
    return reuseTuple;
  }
}
