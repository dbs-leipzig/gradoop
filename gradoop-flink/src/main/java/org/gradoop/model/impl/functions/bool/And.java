package org.gradoop.model.impl.functions.bool;

import org.apache.flink.api.common.functions.CrossFunction;

public class And implements CrossFunction<Boolean, Boolean, Boolean> {
  @Override
  public Boolean cross(Boolean a, Boolean b) throws Exception {
    return a && b;
  }
}
