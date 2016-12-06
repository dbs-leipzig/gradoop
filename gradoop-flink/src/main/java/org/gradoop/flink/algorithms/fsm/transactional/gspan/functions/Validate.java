package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.GSpanKernel;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

public class Validate implements FilterFunction<WithCount<TraversalCode<String>>> {

  private final GSpanKernel gSpan;

  public Validate(GSpanKernel gSpan) {
    this.gSpan = gSpan;
  }

  @Override
  public boolean filter(WithCount<TraversalCode<String>> traversalCodeWithCount) throws Exception {
    TraversalCode<String> code = traversalCodeWithCount.getObject();

    boolean valid = true;

    if (code.getTraversals().size() > 1 ) {
      valid = gSpan.isMinimal(code);
    }
    return valid;
  }
}
