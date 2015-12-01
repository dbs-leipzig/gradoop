package org.gradoop.model.impl.functions.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;

/**
 * left, right => left
 *
 * @param <L> left type
 * @param <R> right type
 */
@FunctionAnnotation.ForwardedFieldsFirst("*->*")
public class LeftSide<L, R> implements JoinFunction<L, R, L> {

  @Override
  public L join(L l, R r) throws Exception {
    return l;
  }
}
