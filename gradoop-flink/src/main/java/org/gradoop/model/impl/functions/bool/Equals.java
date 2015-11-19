package org.gradoop.model.impl.functions.bool;

import org.apache.flink.api.common.functions.CrossFunction;

/**
 * Created by peet on 19.11.15.
 */
public class Equals<T> implements CrossFunction<T, T, Boolean> {

  @Override
  public Boolean cross(T left, T right) throws Exception {
    return left.equals(right);
  }
}
