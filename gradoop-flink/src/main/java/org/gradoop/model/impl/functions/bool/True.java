package org.gradoop.model.impl.functions.bool;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.operators.equality.tuples.DataLabel;

public class True<T> implements MapFunction<T, Boolean> {
  @Override
  public Boolean map(T t) throws Exception {
    return true;
  }
}
