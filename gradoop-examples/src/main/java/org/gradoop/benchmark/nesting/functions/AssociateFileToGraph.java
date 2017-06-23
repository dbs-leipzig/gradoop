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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;

/**
 * Associates to each file name a new graph head, carriying also the information of which operand
 * it belongs to.
 *
 * f0: file name
 * f1: if it belongs to the left operand or not
 * f2: Head associated to the new file
 */
@FunctionAnnotation.ForwardedFields("* -> f0")
public class AssociateFileToGraph
  implements MapFunction<String, Tuple3<String, Boolean, GraphHead>> {

  /**
   * Generating a new graph head for each file that has to be mapped
   */
  private final GraphHeadFactory ghf;

  /**
   * Graph Id
   */
  private int i;

  /**
   * Reusable element
   */
  private final Tuple3<String, Boolean, GraphHead> reusable;

  /**
   * Default constructor
   * @param isLeftOperand   Which operand to these file define
   * @param factory         Factory creating new graph heads
   * @param i               Associating to each GraphHead a label describing it (for debugging)
   */
  public AssociateFileToGraph(boolean isLeftOperand, int i, GraphHeadFactory factory) {
    reusable = new Tuple3<>();
    ghf = factory;
    reusable.f1 = isLeftOperand;
    this.i = i;
  }

  @Override
  public Tuple3<String, Boolean, GraphHead> map(String value) throws Exception {
    reusable.f0 = value;
    reusable.f2 = ghf.createGraphHead();
    reusable.f2.setLabel(i + " ");
    return reusable;
  }


}
