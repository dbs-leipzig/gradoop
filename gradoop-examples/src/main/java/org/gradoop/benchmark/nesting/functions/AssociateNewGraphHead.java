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
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Associates to each different file name
 */
@FunctionAnnotation.ForwardedFields("* -> f0")
public class AssociateNewGraphHead
  implements MapFunction<String, Tuple3<String, Boolean, GraphHead>> {

  /**
   * Reusable element
   */
  private final Tuple3<String, Boolean, GraphHead> toret;

  /**
   * Default constructor
   * @param ghf     The factory generating the heads
   * @param isLake  Defining it this is the main graph or not
   */
  public AssociateNewGraphHead(EPGMGraphHeadFactory<GraphHead> ghf, boolean isLake) {
    toret = new Tuple3<>();
    toret.f1 = isLake;
    toret.f2 = ghf.createGraphHead();
  }

  @Override
  public Tuple3<String, Boolean, GraphHead> map(String s) throws Exception {
    toret.f0 = s;
    return toret;
  }
}
