package org.gradoop.model.impl.functions.bool;

import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * Created by peet on 20.11.15.
 */
public class Or implements ReduceFunction<Boolean> {
  @Override
  public Boolean reduce(Boolean first, Boolean second) throws Exception {
    return first || second;
  }
}
