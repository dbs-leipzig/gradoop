package org.gradoop.model.impl.functions.join;

import org.apache.flink.api.common.functions.JoinFunction;

/**
 * Created by peet on 20.11.15.
 */
public class LeftSideOnly<L, R> implements JoinFunction<L, R, L> {
  @Override
  public L join(L l, R r) throws Exception {
    return l;
  }
}
