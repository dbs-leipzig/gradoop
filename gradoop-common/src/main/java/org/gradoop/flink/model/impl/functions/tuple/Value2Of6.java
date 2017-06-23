package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Forwards the 2nd field of a 6plet
 */
@FunctionAnnotation.ForwardedFields("f2 -> *")
public class Value2Of6<A, B, C, D, E, F> implements KeySelector<Tuple6<A, B, C, D, E, F>, C> {
  @Override
  public C getKey(Tuple6<A, B, C, D, E, F> value) throws Exception {
    return value.f2;
  }
}
