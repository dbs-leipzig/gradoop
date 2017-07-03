
package org.gradoop.flink.model.impl.operators.difference.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Left join, return first field of left tuple 2.
 *
 * @param <O> an object type
 */
@FunctionAnnotation.ForwardedFieldsFirst("*->*")
public class CreateTuple2WithLong<O> implements
  MapFunction<O, Tuple2<O, Long>> {

  /**
   * Reduce instantiations
   */
  private final Tuple2<O, Long> reuseTuple = new Tuple2<>();

  /**
   * Creates this mapper
   *
   * @param secondField user defined long value
   */
  public CreateTuple2WithLong(Long secondField) {
    this.reuseTuple.f1 = secondField;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<O, Long> map(O o) throws Exception {
    reuseTuple.f0 = o;
    return reuseTuple;
  }
}
