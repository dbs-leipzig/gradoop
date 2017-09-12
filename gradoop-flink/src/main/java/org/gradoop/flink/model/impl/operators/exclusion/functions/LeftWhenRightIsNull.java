package org.gradoop.flink.model.impl.operators.exclusion.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;

/**
 * Returns the left side if the right side is null.
 *
 * @param <E> an object type
 */
public class LeftWhenRightIsNull<E> implements FlatJoinFunction<E, E, E> {

  @Override
  public void join(E left, E right, Collector<E> collector) {
    if (left != null && right == null) {
      collector.collect(left);
    }
  }
}
