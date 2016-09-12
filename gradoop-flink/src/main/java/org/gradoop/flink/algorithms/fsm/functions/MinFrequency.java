package org.gradoop.flink.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;

public class MinFrequency implements MapFunction<Long, Long> {

  private final FSMConfig fsmConfig;

  public MinFrequency(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public Long map(Long value) throws Exception {
    return (long) Math.round((float) value * fsmConfig.getMinSupport());
  }
}
