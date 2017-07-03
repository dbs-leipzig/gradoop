
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;

/**
 * Represents an embedding and a weld point to grow the embedding.
 *
 * f0: tie point
 * f1: embedding
 *
 * @param <K> key type
 */
public class EmbeddingWithTiePoint<K> extends Tuple2<K, Embedding<K>> {

  public K getTiePointId() {
    return f0;
  }

  public void setTiePointId(K tiePointId) {
    f0 = tiePointId;
  }

  public Embedding<K> getEmbedding() {
    return f1;
  }

  public void setEmbedding(Embedding<K> embedding) {
    f1 = embedding;
  }
}
