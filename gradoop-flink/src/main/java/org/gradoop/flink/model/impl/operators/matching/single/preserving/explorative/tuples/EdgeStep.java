
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Represents an edge that is joined with an {@link EmbeddingWithTiePoint} to
 * extend it at the tie point.
 *
 * f0: edge id
 * f1: tie point (sourceId/targetId)
 * f2: next id (sourceId/targetId)
 *
 * @param <K> key type
 */
public class EdgeStep<K> extends Tuple3<K, K, K> {

  public K getEdgeId() {
    return f0;
  }

  public void setEdgeId(K edgeId) {
    f0 = edgeId;
  }

  public K getTiePoint() {
    return f1;
  }

  public void setTiePointId(K tiePoint) {
    f1 = tiePoint;
  }

  public K getNextId() {
    return f2;
  }

  public void setNextId(K nextId) {
    f2 = nextId;
  }
}
