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

package org.gradoop.flink.model.impl.operators.fusion.reduce.functions;

import org.apache.flink.api.common.functions.CrossFunction;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Adds the elements to a graph head
 */
public class AddVerticesToNewGraph implements CrossFunction<Vertex, GraphHead, Vertex> {
  @Override
  public Vertex cross(Vertex val1, GraphHead val2) throws Exception {
    val1.addGraphId(val2.getId());
    return val1;
  }
}
