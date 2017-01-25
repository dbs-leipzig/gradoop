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

package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.GraphElement;

/**
 * True, if an element is not contained in a given graph.
 *
 * @param <GE> element type
 */
@FunctionAnnotation.ReadFields("graphIds")
public class InGraphBroadcast<GE extends GraphElement>
  extends GraphContainmentFilterBroadcast<GE> {

  @Override
  public boolean filter(GE element) throws Exception {
    return element.getGraphIds().contains(graphId);
  }
}
