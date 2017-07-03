package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.ExpandEmbedding;

/**
 * Filters results from previous iterations
 */
@FunctionAnnotation.ReadFields("f1")
public class FilterPreviousExpandEmbedding extends RichFilterFunction<ExpandEmbedding> {
  /**
   * super step
   */
  private int currentIteration;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    currentIteration = getIterationRuntimeContext().getSuperstepNumber() * 2 - 1;
  }

  @Override
  public boolean filter(ExpandEmbedding expandEmbedding) {
    return expandEmbedding.pathSize() >= currentIteration;
  }
}
