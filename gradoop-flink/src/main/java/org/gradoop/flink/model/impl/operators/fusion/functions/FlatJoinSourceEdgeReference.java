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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Function updating the edges' sources or destination
 * for each newly created hypervertex
 */
public class FlatJoinSourceEdgeReference implements
  FlatJoinFunction<Edge, Tuple2<Vertex, GradoopId>, Edge> {

  /**
   * Checking if the stuff is actually updating the sources.
   * Otherwise, it updates the targets
   */
  private final boolean isItSourceDoingNow;

  /**
   * Default constructor
   * @param isItSourceDoingNow  If true, it does test the vertices. The targets are tested,
   *                            otherwise
   */
  public FlatJoinSourceEdgeReference(boolean isItSourceDoingNow) {
    this.isItSourceDoingNow = isItSourceDoingNow;
  }

  @Override
  public void join(Edge first, Tuple2<Vertex, GradoopId> second, Collector<Edge> out) throws
    Exception {
    if (second != null && !(second.f1.equals(GradoopId.NULL_VALUE))) {
      if (isItSourceDoingNow) {
        first.setSourceId(second.f1);
      } else {
        first.setTargetId(second.f1);
      }
    }
    out.collect(first);
  }

}
