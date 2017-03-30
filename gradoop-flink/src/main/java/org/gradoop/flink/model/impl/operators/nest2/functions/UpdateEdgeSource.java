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

package org.gradoop.flink.model.impl.operators.nest2.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;

/**
 * Left: old edge information to be mapped into a new edge.
 * Right: new vertex containing the informations of the vertex that will be substituted.
 *
 * As a new result, I create the information for a new edge
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0 -> f0; f2 -> f2; f5 -> f5")
public class UpdateEdgeSource implements JoinFunction<Hexaplet, Hexaplet, Hexaplet> {

  /**
   * Whether the edge's source or target has to be updated
   */
  private final boolean isSource;

  /**
   * Default constructor
   * @param isSource  whether the edge's source or target has to be updated
   */
  public UpdateEdgeSource(boolean isSource) {
    this.isSource = isSource;
  }

  @Override
  public Hexaplet join(Hexaplet edge, Hexaplet vertex) throws Exception {
    if (vertex != null && !(vertex.f1.equals(GradoopId.NULL_VALUE))) {
      // Creates a new edge by creating a new edge id.
      edge.setNewId(GradoopId.get());
      // Updates either the source or the target accordingly to the match phase.
      if (isSource) {
        edge.setSource(vertex.f4);
      } else {
        edge.setTarget(vertex.f4);
      }
    }
    return edge;
  }
}
