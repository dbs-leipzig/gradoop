package org.gradoop.flink.model.impl.functions.utils;

import org.apache.flink.api.common.functions.JoinFunction;

/**
 * Evaluates to true, if one join partner is NULL.
 * @param <L> left type
 * @param <R> right type
 */
public class OneSideEmpty<L, R> implements JoinFunction<L, R, Boolean> {

  @Override
  public Boolean join(L left, R right) throws Exception {
    return left == null || right == null;
  }
}
