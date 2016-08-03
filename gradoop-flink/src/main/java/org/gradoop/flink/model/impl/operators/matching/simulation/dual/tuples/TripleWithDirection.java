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

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Represents an edge-source-target triple and its query candidates.
 *
 * f0: edge id
 * f1: source vertex id
 * f2: target vertex id
 * f3: outgoing edge flag (true if outgoing, false if incoming)
 * f4: query candidates
 */
public class TripleWithDirection extends
  Tuple5<GradoopId, GradoopId, GradoopId, Boolean, boolean[]> {

  public GradoopId getEdgeId() {
    return f0;
  }

  public void setEdgeId(GradoopId edgeId) {
    f0 = edgeId;
  }

  public GradoopId getSourceId() {
    return f1;
  }

  public void setSourceId(GradoopId sourceId) {
    f1 = sourceId;
  }

  public GradoopId getTargetId() {
    return f2;
  }

  public void setTargetId(GradoopId targetId) {
    f2 = targetId;
  }

  public Boolean isOutgoing() {
    return f3;
  }

  public void setOutgoing(Boolean outgoing) {
    f3 = outgoing;
  }

  public boolean[] getCandidates() {
    return f4;
  }

  public void setCandidates(boolean[] candidates) {
    f4 = candidates;
  }
}
