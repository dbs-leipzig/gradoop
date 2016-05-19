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

package org.gradoop.model.impl.operators.matching.common.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.impl.id.GradoopId;

import java.util.List;

/**
 * Represents an edge, source and target vertex triple that matches at least one
 * triple in the data graph. Each triple contains a list of identifiers that
 * match to edge ids in the query graph.

 * f0: edge id
 * f1: source vertex id
 * f2: target vertex id
 * f3: query candidates
 */
public class MatchingTriple
  extends Tuple4<GradoopId, GradoopId, GradoopId, List<Long>> {

  public GradoopId getEdgeId() {
    return f0;
  }

  public void setEdgeId(GradoopId id) {
    f0 = id;
  }

  public GradoopId getSourceVertexId() {
    return f1;
  }

  public void setSourceVertexId(GradoopId id) {
    f1 = id;
  }

  public GradoopId getTargetVertexId() {
    return f2;
  }

  public void setTargetVertexId(GradoopId id) {
    f2 = id;
  }

  public List<Long> getQueryCandidates() {
    return f3;
  }

  public void setQueryCandidates(List<Long> ids) {
    f3 = ids;
  }
}
