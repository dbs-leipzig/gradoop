package org.gradoop.model.impl.functions.bool;

import org.apache.flink.api.common.functions.MapFunction;

public class Not implements MapFunction<Boolean, Boolean> {

  @Override
  public Boolean map(Boolean b) throws Exception {
    return !b;
  }
}
