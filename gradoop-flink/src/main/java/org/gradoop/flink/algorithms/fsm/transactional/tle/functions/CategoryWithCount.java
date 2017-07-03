
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.CCSGraph;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * graph => (category, 1)
 */
public class CategoryWithCount
  implements MapFunction<CCSGraph, WithCount<String>> {

  /**
   * reuse tuple to avoid instantiations
   */
  private WithCount<String> reuseTuple = new WithCount<>(null);

  @Override
  public WithCount<String> map(CCSGraph value) throws Exception {
    reuseTuple.setObject(value.getCategory());
    return reuseTuple;
  }
}
