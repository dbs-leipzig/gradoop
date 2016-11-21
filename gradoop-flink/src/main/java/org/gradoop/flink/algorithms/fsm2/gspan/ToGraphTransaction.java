package org.gradoop.flink.algorithms.fsm2.gspan;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.representation.RepresentationConverters;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

/**
 * Created by peet on 21.11.16.
 */
public class ToGraphTransaction implements MapFunction<TraversalCode<String>, GraphTransaction> {
  @Override
  public GraphTransaction map(TraversalCode<String> stringTraversalCode) throws Exception {
    return RepresentationConverters.createGraphTransaction(stringTraversalCode);
  }
}
