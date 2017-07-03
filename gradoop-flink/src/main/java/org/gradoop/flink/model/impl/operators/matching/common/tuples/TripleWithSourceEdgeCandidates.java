
package org.gradoop.flink.model.impl.operators.matching.common.tuples;

import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Represents a triple with source and edge candidates.
 *
 * f0: edge id
 * f1: source vertex id
 * f2: source vertex candidates
 * f3: target vertex id
 * f4: edge candidates
 *
 * @param <K> key type
 */
public class TripleWithSourceEdgeCandidates<K> extends Tuple5<K, K, boolean[], K, boolean[]> {

  public K getEdgeId() {
    return f0;
  }

  public void setEdgeId(K edgeId) {
    f0 = edgeId;
  }

  public K getSourceId() {
    return f1;
  }

  public void setSourceId(K sourceId) {
    f1 = sourceId;
  }

  public boolean[] getSourceCandidates() {
    return f2;
  }

  public void setSourceCandidates(boolean[] sourceCandidates) {
    f2 = sourceCandidates;
  }

  public K getTargetId() {
    return f3;
  }

  public void setTargetId(K targetId) {
    f3 = targetId;
  }

  public boolean[] getEdgeCandidates() {
    return f4;
  }

  public void setEdgeCandidates(boolean[] edgeCandidates) {
    f4 = edgeCandidates;
  }
}
