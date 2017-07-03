
package org.gradoop.flink.model.impl.operators.statistics.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Returns the first value of the join pair if it exists, otherwise a new {@link WithCount} with
 * count = 0 is created.
 */
public class SetOrCreateWithCount
  implements JoinFunction<WithCount<GradoopId>, GradoopId, WithCount<GradoopId>> {
  /**
   * Reduce object instantiations
   */
  private final WithCount<GradoopId> reuseTuple;
  /**
   * Constructor
   */
  public SetOrCreateWithCount() {
    reuseTuple = new WithCount<>();
    reuseTuple.setCount(0L);
  }

  @Override
  public WithCount<GradoopId> join(WithCount<GradoopId> first, GradoopId second) throws Exception {
    if (first == null) {
      reuseTuple.setObject(second);
      return reuseTuple;
    } else {
      return first;
    }
  }
}
