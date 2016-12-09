/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
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
