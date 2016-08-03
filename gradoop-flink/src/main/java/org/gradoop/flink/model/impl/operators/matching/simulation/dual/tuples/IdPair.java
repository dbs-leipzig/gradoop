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

package org.gradoop.flink.model.impl.operators.matching.simulation.dual.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * A pair of {@link org.gradoop.model.impl.id.GradoopId} representing an edge
 * identifier and a target vertex identifier.
 */
public class IdPair extends Tuple2<GradoopId, GradoopId> {

  public GradoopId getEdgeId() {
    return f0;
  }

  public void setEdgeId(GradoopId first) {
    f0 = first;
  }

  public GradoopId getTargetId() {
    return f1;
  }

  public void setTargetId(GradoopId second) {
    f1 = second;
  }
}
