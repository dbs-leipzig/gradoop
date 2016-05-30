package org.gradoop.model.impl.operators.matching.common.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Represents an embedding of a query pattern in the search graph.
 *
 * f0: vertex embeddings
 * f1: edge embeddings
 */
public class Embedding extends Tuple2<GradoopId[], GradoopId[]> {

  public GradoopId[] getVertexEmbeddings() {
    return f0;
  }

  public void setVertexEmbeddings(GradoopId[] vertexEmbedding) {
    f0 = vertexEmbedding;
  }

  public GradoopId[] getEdgeEmbeddings() {
    return f1;
  }

  public void setEdgeEmbeddings(GradoopId[] edgeEmbedding) {
    f1 = edgeEmbedding;
  }
}
