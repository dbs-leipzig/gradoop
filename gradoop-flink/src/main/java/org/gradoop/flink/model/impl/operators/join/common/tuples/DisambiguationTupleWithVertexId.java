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

package org.gradoop.flink.model.impl.operators.join.common.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Tuple representing the fused vertex, and eventually its id (boolean)
 * in the graph operand
 *
 * f0: fused vertex
 * f1: if f2 appears in the graph operand
 * f2: operand's vertex id matching with f0.
 *
 * Created by Giacomo Bergami on 16/02/17.
 */
public class DisambiguationTupleWithVertexId extends Tuple3<Vertex, Boolean, GradoopId> {
  /**
   * Default constructor
   */
  public DisambiguationTupleWithVertexId() {
    super();
  }

}
