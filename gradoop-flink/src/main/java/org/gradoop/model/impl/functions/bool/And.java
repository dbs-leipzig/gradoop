package org.gradoop.model.impl.functions.bool;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * Logical and as Flink function
 */
public class And implements CrossFunction<Boolean, Boolean, Boolean>,
  ReduceFunction<Boolean> {

  @Override
  public Boolean cross(Boolean a, Boolean b) throws Exception {
    return a && b;
  }

  @Override
  public Boolean reduce(Boolean a, Boolean b) throws Exception {
    return a && b;
  }
}
