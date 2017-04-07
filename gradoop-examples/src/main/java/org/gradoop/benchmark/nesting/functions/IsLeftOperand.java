package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Returns the head belonging to the main graph
 */
@FunctionAnnotation.ForwardedFields("f1 -> *")
public class IsLeftOperand implements FilterFunction<Tuple3<String, Boolean, GraphHead>> {

  /**
   * Operand selector
   */
  public final boolean isLeft;

  /**
   * The behaviour of the class changes accordingly to its parameter
   * @param getLeftOperand  Set to true if you want to select the left operand, to false otherwise
   */
  public IsLeftOperand(boolean getLeftOperand) {
    isLeft = getLeftOperand;
  }

  @Override
  public boolean filter(Tuple3<String, Boolean, GraphHead> stringBooleanGraphHeadTuple3) throws
    Exception {
    return stringBooleanGraphHeadTuple3.f1 && isLeft;
  }
}
