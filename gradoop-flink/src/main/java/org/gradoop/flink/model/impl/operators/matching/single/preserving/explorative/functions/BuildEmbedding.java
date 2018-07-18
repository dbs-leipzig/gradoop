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
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions;

import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;

import java.io.Serializable;
import java.lang.reflect.Array;

/**
 * Base class for building initial embeddings from either vertices or edge triples.
 *
 * @param <K> key type
 */
abstract class BuildEmbedding<K> implements Serializable {
  /**
   * Reduce instantiations
   */
  protected final Embedding<K> reuseEmbedding;
  /**
   * Reduce instantiations
   */
  protected final EmbeddingWithTiePoint<K> reuseEmbeddingWithTiePoint;

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
