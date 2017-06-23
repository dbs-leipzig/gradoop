package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Extracts the first field of a 6plet
 */
@FunctionAnnotation.ForwardedFields("f0 -> *")
public class Value0Of6<A, B, C, D, E, F> implements KeySelector<Tuple6<A, B, C, D, E, F>, A> {
  @Override
  public A getKey(Tuple6<A, B, C, D, E, F> value) throws Exception {
    return value.f0;
  }
}
