package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Created by vasistas on 08/04/17.
 */
public class Value1Of3AsFilter implements FilterFunction<Tuple3<String, Boolean, GraphHead>> {

  private final boolean isLeftOperand;

  public Value1Of3AsFilter(boolean isLeftOperand) {
    this.isLeftOperand = isLeftOperand;
  }

  @Override
  public boolean filter(Tuple3<String, Boolean, GraphHead> value) throws Exception {
    return value.f1 == isLeftOperand;
  }
}
