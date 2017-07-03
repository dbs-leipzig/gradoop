
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples;

import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Represents a vertex that is joined with an {@link EmbeddingWithTiePoint}
 * to extend it at the tie point.
 *
 * @param <K> key type
 */
public class VertexStep<K> extends Tuple1<K> {

  public K getVertexId() {
    return f0;
  }

  public void setVertexId(K vertexId) {
    f0 = vertexId;
  }
}
