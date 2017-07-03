
package org.gradoop.flink.model.impl.operators.matching.common.tuples;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Represents an edge, source and target vertex triple that matches at least one
 * triple in the data graph. Each triple contains a list of identifiers that
 * match to edge ids in the query graph.

 * f0: edge id
 * f1: source vertex id
 * f2: target vertex id
 * f3: edge query candidates
 *
 * @param <K> key type
 */
public class TripleWithCandidates<K> extends Tuple4<K, K, K, boolean[]> {

  public K getEdgeId() {
    return f0;
  }

  public void setEdgeId(K id) {
    f0 = id;
  }

  public K getSourceId() {
    return f1;
  }

  public void setSourceId(K id) {
    f1 = id;
  }

  public K getTargetId() {
    return f2;
  }

  public void setTargetId(K id) {
    f2 = id;
  }

  public boolean[] getCandidates() {
    return f3;
  }

  public void setCandidates(boolean[] candidates) {
    f3 = candidates;
  }
}
