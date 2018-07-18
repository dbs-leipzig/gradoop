/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples;

import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.List;
import java.util.Map;

/**
 * Represents a vertex and its neighborhood.
 *
 * f0: vertex id
 * f1: vertex query candidates
 * f2: parent ids
 * f3: counters for incoming edge candidates
 * f4: outgoing edges (edgeId, targetId) and their query candidates
 * f5: updated flag
 */
public class FatVertex extends Tuple6<GradoopId, List<Long>, List<GradoopId>,
    int[], Map<IdPair, boolean[]>, Boolean> {

  public GradoopId getVertexId() {
    return f0;
  }

  public void setVertexId(GradoopId vertexId) {
    f0 = vertexId;
  }

  public List<Long> getCandidates() {
    return f1;
  }

  public void setCandidates(List<Long> candidates) {
    f1 = candidates;
  }

  public List<GradoopId> getParentIds() {
    return f2;
  }

  public void setParentIds(List<GradoopId> parentIds) {
    f2 = parentIds;
  }

  public int[] getIncomingCandidateCounts() {
    return f3;
  }

  public void setIncomingCandidateCounts(int[] incomingCandidateCounts) {
    f3 = incomingCandidateCounts;
  }

  public Map<IdPair, boolean[]> getEdgeCandidates() {
    return f4;
  }

  public void setEdgeCandidates(Map<IdPair, boolean[]> edgeCandidates) {
    f4 = edgeCandidates;
  }

  public Boolean isUpdated() {
    return f5;
  }

  public void setUpdated(boolean updated) {
    f5 = updated;
  }
}
