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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.Step;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;

/**
 * Initializes an {@link EmbeddingWithTiePoint} from the given edge triple.
 *
 * @param <K> key type
 */
public class BuildEmbeddingFromTriple<K>
  extends BuildEmbedding<K>
  implements FlatMapFunction<TripleWithCandidates<K>, EmbeddingWithTiePoint<K>> {
  /**
   * Match strategy for the traversal.
   */
  private final MatchStrategy matchStrategy;
  /**
   * Index of the source vertex in the vertex embedding.
   */
  private final int sourceIndex;
  /**
   * Index of the edge in the edge embedding
   */
  private final int edgeIndex;
  /**
   * Index of the target vertex in the vertex mapping.
   */
  private final int targetIndex;
  /**
   * Vertex candidate to continue traversal from
   */
  private int nextFrom;
  /**
   * True, iff the initial step is a loop.
   */
  private boolean isQueryLoop;

  /**
   * Constructor
   *
   * @param keyClazz    key type is needed for array initialization
   * @param traversalCode traversal code for the current query
   * @param matchStrategy strategy used for morphism checks
   * @param vertexCount number of vertices in the query graph
   * @param edgeCount   number of edges in the query graph
   */
  public BuildEmbeddingFromTriple(Class<K> keyClazz, TraversalCode traversalCode,
    MatchStrategy matchStrategy, long vertexCount, long edgeCount) {
    super(keyClazz, vertexCount, edgeCount);

    this.matchStrategy = matchStrategy;
    Step step = traversalCode.getStep(0);

    boolean isOutgoing = step.isOutgoing();
    this.edgeIndex = (int) step.getVia();
    // set source and target index in case of incoming or outgoing traversal
    this.sourceIndex = isOutgoing ? (int) step.getFrom() : (int) step.getTo();
    this.targetIndex = isOutgoing ? (int) step.getTo() : (int) step.getFrom();
    this.isQueryLoop = sourceIndex == targetIndex;

    if (traversalCode.getSteps().size() > 1) {
      nextFrom = (int) traversalCode.getStep(1).getFrom();
    }
  }

  @Override
  public void flatMap(TripleWithCandidates<K> triple, Collector<EmbeddingWithTiePoint<K>> out)
    throws Exception {
    if (isValidTriple(triple)) {
      reuseEmbedding.getEdgeMapping()[edgeIndex] = triple.getEdgeId();
      reuseEmbedding.getVertexMapping()[sourceIndex] = triple.getSourceId();
      reuseEmbedding.getVertexMapping()[targetIndex] = triple.getTargetId();
      reuseEmbeddingWithTiePoint.setTiePointId(reuseEmbedding.getVertexMapping()[nextFrom]);
      out.collect(reuseEmbeddingWithTiePoint);
    }
  }

  /**
   * Checks if the given triple is valid according to the match strategy and query characteristics.
   *
   * @param triple triple to check validity for
   * @return true, iff the triple is a valid initial candidate
   */
  private boolean isValidTriple(TripleWithCandidates<K> triple) {
    return (matchStrategy == MatchStrategy.HOMOMORPHISM) ||
      (isQueryLoop && triple.getSourceId().equals(triple.getTargetId())) ||
      (!isQueryLoop && !triple.getSourceId().equals(triple.getTargetId()));
  }
}
