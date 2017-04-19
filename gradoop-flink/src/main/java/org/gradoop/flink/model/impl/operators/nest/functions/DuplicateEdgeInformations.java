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

package org.gradoop.flink.model.impl.operators.nest.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;

/**
 * This method is used to create new edges and copy old edges
 * in order to create new ones
 */
public class DuplicateEdgeInformations implements JoinFunction<Edge, Hexaplet, Edge> {

  @Override
  public Edge join(Edge e, Hexaplet y) throws
    Exception {
    // There should be just one edge with the same id
    e.setId(y.f4);
    e.setSourceId(y.getSource());
    e.setTargetId(y.getTarget());
    return e;
  }

}
