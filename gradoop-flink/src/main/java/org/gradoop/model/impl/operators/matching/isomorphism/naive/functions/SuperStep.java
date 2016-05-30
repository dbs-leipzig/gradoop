package org.gradoop.model.impl.operators.matching.isomorphism.naive.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.tuples.EmbeddingWithTiePoint;

/**
 * Copyright 2016 martin.
 */
public class SuperStep extends RichMapFunction<EmbeddingWithTiePoint, Integer> {

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
