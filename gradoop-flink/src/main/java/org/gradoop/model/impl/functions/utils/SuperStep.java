package org.gradoop.model.impl.functions.utils;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.tuples.EmbeddingWithTiePoint;

/**
 * Returns the current super step using {@link IterationRuntimeContext}.
 */
public class SuperStep extends RichMapFunction<EmbeddingWithTiePoint, Integer> {

  /**
   * super step
   */
  private Integer superstep;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    superstep = getIterationRuntimeContext().getSuperstepNumber();
  }

  @Override
  public Integer map(EmbeddingWithTiePoint value) throws Exception {
    return superstep;
  }
}
