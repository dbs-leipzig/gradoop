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

package org.gradoop.flink.model.impl.operators.matching.common.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Represents an EPGM graph element (vertex/edge) and its query candidates. The
 * query candidates are represented by a bit vector.
 *
 * f0: EPGM graph element id
 * f1: query candidates
 */
public class IdWithCandidates extends Tuple2<GradoopId, boolean[]> {

  public GradoopId getId() {
    return f0;
  }

  public void setId(GradoopId id) {
    f0 = id;
  }

  public boolean[] getCandidates() {
    return f1;
  }

  public void setCandidates(boolean[] candidates) {
    f1 = candidates;
  }
}
