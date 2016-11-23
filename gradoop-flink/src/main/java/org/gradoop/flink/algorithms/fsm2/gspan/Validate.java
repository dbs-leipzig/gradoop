package org.gradoop.flink.algorithms.fsm2.gspan;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

public class Validate implements FilterFunction<WithCount<TraversalCode<String>>> {

  @Override
  public boolean filter(WithCount<TraversalCode<String>> traversalCodeWithCount) throws Exception {
    TraversalCode<String> code = traversalCodeWithCount.getObject();

    return code.getTraversals().size() == 1 || GSpan.isMinimal(code);
  }
}
