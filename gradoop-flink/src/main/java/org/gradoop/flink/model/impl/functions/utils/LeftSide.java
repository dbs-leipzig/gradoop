
package org.gradoop.flink.model.impl.functions.utils;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;

/**
 * left, right => left
 *
 * @param <L> left type
 * @param <R> right type
 */
@FunctionAnnotation.ForwardedFieldsFirst("*->*")
public class LeftSide<L, R>
  implements CrossFunction<L, R, L>, JoinFunction<L, R, L>, CoGroupFunction<L, R, L> {

  @Override
  public L cross(L left, R right) throws Exception {
    return left;
  }

  @Override
  public L join(L first, R second) throws Exception {
    return first;
  }

  @Override
  public void coGroup(Iterable<L> first, Iterable<R> second, Collector<L> out) throws Exception {
    if (second.iterator().hasNext()) {
      for (L x : first) {
        out.collect(x);
      }
    }
  }
}
