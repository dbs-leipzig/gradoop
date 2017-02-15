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

package org.gradoop.flink.model.impl.operators.grouping.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * f0: vertex id
 * f1: group representative vertex id
 * f2: connected group representative edge id
 * f3: true if vertex is source of this edge
 */
public class VertexWithSuperVertexAndEdge extends Tuple4<GradoopId, GradoopId, GradoopId, Boolean> {

  public GradoopId getVertexId() {
    return f0;
  }

  public void setVertexId(GradoopId vertexId) {
    f0 = vertexId;
  }

  public GradoopId getSuperVertexId() {
    return f1;
  }

  public void setSuperVertexId(GradoopId superVertexId) {
    f1 = superVertexId;
  }

  public GradoopId getSuperEdgeId() {
    return f2;
  }

  public void setSuperEdgeId(GradoopId superEdgeId) {
    f2 = superEdgeId;
  }

  public boolean isSource() {
    return f3;
  }

  public void setSource(Boolean isSource) {
    f3 = isSource;
  }
}
