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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;



import java.lang.reflect.Array;

/**
 * Initializes an {@link EmbeddingWithTiePoint} from the given vertex.
 *
 * Forwarded fields:
 *
 * f0: vertex id -> tie point
 *
 * @param <K> key type
 */
@FunctionAnnotation.ForwardedFields("f0")
public class BuildEmbeddingWithTiePoint<K>
  implements MapFunction<IdWithCandidates<K>, EmbeddingWithTiePoint<K>> {
  /**
   * Reduce instantiations
   */
  private final Embedding<K> reuseEmbedding;
  /**
   * Reduce instantiations
   */
  private final EmbeddingWithTiePoint<K> reuseEmbeddingWithTiePoint;

  /**
   * Vertex candidate to start with
   */
  private final int candidate;

  /**
   * Constructor
   *
   * @param keyClazz      key type is needed for array initialization
   * @param candidate     initial query candidate each vertex is mapped to
   * @param vertexCount   number of vertices in the query graph
   * @param edgeCount     number of edges in the query graph
   */
  public BuildEmbeddingWithTiePoint(Class<K> keyClazz, int candidate,
    long vertexCount, long edgeCount) {
    this.candidate              = candidate;
    reuseEmbedding              = new Embedding<>();
    reuseEmbeddingWithTiePoint  = new EmbeddingWithTiePoint<>();
    //noinspection unchecked
    reuseEmbedding.setVertexMappings(
      (K[]) Array.newInstance(keyClazz, (int) vertexCount));
    //noinspection unchecked
    reuseEmbedding.setEdgeMappings(
      (K[]) Array.newInstance(keyClazz, (int) edgeCount));
    reuseEmbeddingWithTiePoint.setEmbedding(reuseEmbedding);
  }

  @Override
  public EmbeddingWithTiePoint<K> map(IdWithCandidates<K> v)
      throws Exception {
    reuseEmbeddingWithTiePoint.setTiePointId(v.getId());
    // candidate is same for all vertices
    reuseEmbedding.getVertexMappings()[candidate] = v.getId();
    return reuseEmbeddingWithTiePoint;
  }
}
