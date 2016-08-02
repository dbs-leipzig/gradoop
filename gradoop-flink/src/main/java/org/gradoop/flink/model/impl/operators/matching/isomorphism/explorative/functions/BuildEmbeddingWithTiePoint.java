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

package org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.matching.common.query
  .TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.tuples
  .IdWithCandidates;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.tuples.EmbeddingWithTiePoint;

/**
 * Initializes an {@link EmbeddingWithTiePoint} from the given vertex.
 *
 * Forwarded fields:
 *
 * f0->f2: vertex id -> weld point
 *
 */
@FunctionAnnotation.ForwardedFields("f0->f1")
public class BuildEmbeddingWithTiePoint
  implements MapFunction<IdWithCandidates, EmbeddingWithTiePoint> {
  /**
   * Reduce instantiations
   */
  private final Embedding reuseEmbedding;
  /**
   * Reduce instantiations
   */
  private final EmbeddingWithTiePoint reuseEmbeddingWithTiePoint;
  /**
   * Number of vertices in the query graph
   */
  private final int vertexCount;
  /**
   * Vertex candidate to start with
   */
  private final int candidate;

  /**
   * Constructor
   *
   * @param traversalCode traversal code for the current exploration
   * @param vertexCount   number of vertices in the query graph
   * @param edgeCount     number of edges in the query graph
   */
  public BuildEmbeddingWithTiePoint(TraversalCode traversalCode,
    long vertexCount, long edgeCount) {
    this.vertexCount            = (int) vertexCount;
    this.candidate              = (int) traversalCode.getStep(0).getFrom();
    reuseEmbedding              = new Embedding();
    reuseEmbeddingWithTiePoint  = new EmbeddingWithTiePoint();
    reuseEmbedding.setEdgeMappings(new GradoopId[(int) edgeCount]);
    reuseEmbeddingWithTiePoint.setEmbedding(reuseEmbedding);
  }

  @Override
  public EmbeddingWithTiePoint map(IdWithCandidates v) throws Exception {
    reuseEmbeddingWithTiePoint.setTiePointId(v.getId());
    GradoopId[] vertexEmbeddings = new GradoopId[vertexCount];
    vertexEmbeddings[candidate] = v.getId();
    reuseEmbedding.setVertexMappings(vertexEmbeddings);
    return reuseEmbeddingWithTiePoint;
  }
}
