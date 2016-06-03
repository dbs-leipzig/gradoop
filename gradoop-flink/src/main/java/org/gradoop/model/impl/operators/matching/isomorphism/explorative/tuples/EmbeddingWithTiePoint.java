package org.gradoop.model.impl.operators.matching.isomorphism.explorative.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.common.tuples.Embedding;

/**
 * Represents an embedding and a weld point to grow the embedding.
 *
 * f0: embedding
 * f1: tie point
 */
public class EmbeddingWithTiePoint
  extends Tuple2<Embedding, GradoopId> {

  public Embedding getEmbedding() {
    return f0;
  }

  public void setEmbedding(Embedding embedding) {
    f0 = embedding;
  }

  public GradoopId getTiePointId() {
    return f1;
  }

  public void setTiePointId(GradoopId tiePointId) {
    f1 = tiePointId;
  }

}
