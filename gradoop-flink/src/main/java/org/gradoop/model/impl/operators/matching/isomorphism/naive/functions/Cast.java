package org.gradoop.model.impl.operators.matching.isomorphism.naive.functions;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Copyright 2016 martin.
 */
public class Cast<IN, T> implements MapFunction<IN, T> {

  private final Class<T> clazz;

  public Cast(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public T map(IN value) throws Exception {
    return clazz.cast(value);
  }
}
