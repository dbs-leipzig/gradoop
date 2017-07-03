
package org.gradoop.flink.model.impl.functions.utils;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;

/**
 * left, right => right
 *
 * @param <L> left type
 * @param <R> right type
 */
@FunctionAnnotation.ForwardedFieldsSecond("*->*")
public class RightSide<L, R> implements JoinFunction<L, R, R>, CrossFunction<L, R, R> {

  @Override
  public R join(L left, R right) throws Exception {
    return right;
  }

  @Override
  public R cross(L left, R right) throws Exception {
    return right;
  }
}
