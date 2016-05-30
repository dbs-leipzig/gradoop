package org.gradoop.model.impl.operators.matching.isomorphism.naive.functions;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Checks if a given object is instance of a specific class.
 */
public class IsInstance<IN, T> implements FilterFunction<IN> {

  private final Class<T> clazz;

  public IsInstance(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public boolean filter(IN value) throws Exception {
    return clazz.isInstance(value);
  }
}
