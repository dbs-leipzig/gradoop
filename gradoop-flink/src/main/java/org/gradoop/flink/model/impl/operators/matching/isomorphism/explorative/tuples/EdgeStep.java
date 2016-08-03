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

package org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Represents an edge that is joined with an {@link EmbeddingWithTiePoint} to
 * extend it at the tie point.
 *
 * f0: edge id
 * f1: tie point (sourceId/targetId)
 * f2: next id (sourceId/targetId)
 */
public class EdgeStep extends Tuple3<GradoopId, GradoopId, GradoopId> {

  public GradoopId getEdgeId() {
    return f0;
  }

  public void setEdgeId(GradoopId edgeId) {
    f0 = edgeId;
  }

  public GradoopId getTiePoint() {
    return f1;
  }

  public void setTiePointId(GradoopId tiePoint) {
    f1 = tiePoint;
  }

  public GradoopId getNextId() {
    return f2;
  }

  public void setNextId(GradoopId nextId) {
    f2 = nextId;
  }
}
