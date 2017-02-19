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

package org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.DisambiguationTupleWithVertexId;
/**
 * Given a vertex and an edge, coming that
 *
 * Created by Giacomo Bergami on 15/02/17.
 */
public class JoinFunctionWithVertexAndGradoopIdFromEdge implements
  JoinFunction<Vertex, Edge, DisambiguationTupleWithVertexId> {

  /**
   * Reusable element to be returned when needed
   */
  private final DisambiguationTupleWithVertexId reusable;

  /**
   * Default constructor
   */
  public JoinFunctionWithVertexAndGradoopIdFromEdge() {
    reusable = new DisambiguationTupleWithVertexId();
  }

  @Override
  public DisambiguationTupleWithVertexId join(Vertex first, Edge second) throws
    Exception {
    reusable.f0 = first;
    reusable.f1 = second != null;
    reusable.f2 = second == null ? GradoopId.NULL_VALUE : second.getId();
    return reusable;
  }
}
