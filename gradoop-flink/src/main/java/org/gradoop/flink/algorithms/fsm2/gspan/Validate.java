package org.gradoop.flink.algorithms.fsm2.gspan;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

/**
 * Created by peet on 21.11.16.
 */
public class Validate implements FilterFunction<WithCount<TraversalCode<String>>> {

  @Override
  public boolean filter(WithCount<TraversalCode<String>> traversalCodeWithCount) throws Exception {
    return true;
  }
}
