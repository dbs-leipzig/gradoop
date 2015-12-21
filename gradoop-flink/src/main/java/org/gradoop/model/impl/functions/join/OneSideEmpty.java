package org.gradoop.model.impl.functions.join;

import org.apache.flink.api.common.functions.JoinFunction;

public class OneSideEmpty<L, R> implements JoinFunction<L, R, Boolean> {

  @Override
  public Boolean join(L left, R right) throws Exception {
    return left == null || right == null;
  }
}
