package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.GSpan;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

public class Validate implements FilterFunction<WithCount<TraversalCode<String>>> {

  private final GSpan gSpan;

  public Validate(GSpan gSpan) {
    this.gSpan = gSpan;
  }

  @Override
  public boolean filter(WithCount<TraversalCode<String>> traversalCodeWithCount) throws Exception {
    TraversalCode<String> code = traversalCodeWithCount.getObject();

    return code.getTraversals().size() == 1 || gSpan.isMinimal(code);
  }
}
