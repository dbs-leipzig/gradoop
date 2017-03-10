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

package org.gradoop.flink.model.impl.nested.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.nested.tuples.Quad;

/**
 * Created by vasistas on 09/03/17.
 */
public class UpdateEdgeSource implements JoinFunction<Quad, Quad, Quad> {

  private final boolean isSource;

  public UpdateEdgeSource(boolean isSource) {
    this.isSource = isSource;
  }

  @Override
  public Quad join(Quad edge, Quad vertex) throws Exception {
    if (vertex != null && !(vertex.f1.equals(GradoopId.NULL_VALUE))) {
      edge.setNewId(GradoopId.get());
      if (isSource) {
        edge.setSource(vertex.f4);
      } else {
        edge.setTarget(vertex.f4);
      }
    }
    return edge;
  }
}
