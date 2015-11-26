package org.gradoop.model.impl.functions.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;

@FunctionAnnotation.ForwardedFieldsFirst("*->*")
public class LeftSide<L, R> implements JoinFunction<L, R, L> {
  @Override
  public L join(L l, R r) throws Exception {
    return l;
  }
}
