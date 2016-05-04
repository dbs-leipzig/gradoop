package org.gradoop.model.impl.operators.matching.simulation.dual.tuples;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.model.impl.id.GradoopId;

import java.util.List;

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
  Tuple5<GradoopId, GradoopId, GradoopId, Boolean, List<Long>> {

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

  public void isOutgoing(Boolean isOutgoing) {
    f3 = isOutgoing;
  }

  public List<Long> getCandidates() {
    return f4;
  }

  public void setCandidates(List<Long> candidates) {
    f4 = candidates;
  }
}
