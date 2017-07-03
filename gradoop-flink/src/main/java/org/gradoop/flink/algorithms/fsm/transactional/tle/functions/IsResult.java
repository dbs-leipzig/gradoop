
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.SubgraphEmbeddings;

/**
 * Filters embeddings or collected results.
 *
 * @param <SE> subgraph embeddings type
 */
public class IsResult<SE extends SubgraphEmbeddings>
  implements FilterFunction<SE> {

  /**
   * Flag, if embeddings (false) or collected results should be filtered (true).
   */
  private final boolean shouldBeResult;

  /**
   * Constructor.
   *
   * @param shouldBeResult filter mode flag
   */
  public IsResult(boolean shouldBeResult) {
    this.shouldBeResult = shouldBeResult;
  }

  @Override
  public boolean filter(SE embeddings) throws Exception {
    boolean isResult = embeddings.getGraphId().equals(GradoopId.NULL_VALUE);

    return shouldBeResult == isResult;
  }
}
