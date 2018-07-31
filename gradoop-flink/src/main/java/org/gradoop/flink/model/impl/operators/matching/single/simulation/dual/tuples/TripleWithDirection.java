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
