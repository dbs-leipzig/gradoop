package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions;

import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;

import java.io.Serializable;
import java.lang.reflect.Array;

/**
 * Base class for building initial embeddings from either vertices or edge triples.
 */
abstract class BuildEmbedding<K> implements Serializable {
  /**
   * Reduce instantiations
   */
  final Embedding<K> reuseEmbedding;
  /**
   * Reduce instantiations
   */
  final EmbeddingWithTiePoint<K> reuseEmbeddingWithTiePoint;

  /**
   * Constructor
   *
   * @param keyClazz      key type is needed for array initialization
   * @param vertexCount   number of vertices in the query graph
   * @param edgeCount     number of edges in the query graph
   */
  BuildEmbedding(Class<K> keyClazz, long vertexCount, long edgeCount) {
    reuseEmbedding              = new Embedding<>();
    reuseEmbeddingWithTiePoint  = new EmbeddingWithTiePoint<>();
    //noinspection unchecked
    reuseEmbedding.setVertexMapping(
      (K[]) Array.newInstance(keyClazz, (int) vertexCount));
    //noinspection unchecked
    reuseEmbedding.setEdgeMapping(
      (K[]) Array.newInstance(keyClazz, (int) edgeCount));
    reuseEmbeddingWithTiePoint.setEmbedding(reuseEmbedding);
  }
}
