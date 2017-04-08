/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Returns the head belonging to the main graph
 */
public class IsLeftOperand implements FilterFunction<Tuple3<String, Boolean, GraphHead>> {

  /**
   * Operand selector
   */
  private final boolean isLeft;

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
