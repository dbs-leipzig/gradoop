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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.Step;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;

/**
 * Extends an embedding with an edge and its source and target vertices if possible.
 *
 * @param <K> key type
 */
public class UpdateVertexEdgeMapping<K>
  extends UpdateMapping<K>
  implements
  FlatJoinFunction<EmbeddingWithTiePoint<K>, TripleWithCandidates<K>, EmbeddingWithTiePoint<K>> {
  /**
   * Query candidate for the source vertex
   */
  private final int sourceCandidate;
  /**
   * Query candidate for the edge
   */
  private final int edgeCandidate;
  /**
   * Query candidate for the target vertex
   */
  private final int targetCandidate;
  /**
   * From field of the next traversal step (if there is one)
   */
  private int nextFrom;

  /**
   * Constructor
   *
   * @param traversalCode traversal code for the current query
   * @param currentStepId step if of the current traversal step
   * @param matchStrategy strategy for morphism testing
   */
  public UpdateVertexEdgeMapping(TraversalCode traversalCode, int currentStepId,
    MatchStrategy matchStrategy) {
    super(traversalCode, matchStrategy);

    setCurrentStepId(currentStepId);
    initializeVisited();

    Step currentStep = getCurrentStep();
    boolean isOutgoing = currentStep.isOutgoing();
    this.edgeCandidate = (int) currentStep.getVia();
    this.sourceCandidate = isOutgoing ? (int) currentStep.getFrom() : (int) currentStep.getTo();
    this.targetCandidate = isOutgoing ? (int) currentStep.getTo() : (int) currentStep.getFrom();

    this.nextFrom = getNextFrom();
  }

  @Override
  public void join(EmbeddingWithTiePoint<K> embedding, TripleWithCandidates<K> t,
    Collector<EmbeddingWithTiePoint<K>> out) throws Exception {

    K edgeId = t.getEdgeId();
    K[] edgeMapping = embedding.getEmbedding().getEdgeMapping();

    if (isValidEdge(edgeId, edgeMapping, edgeCandidate)) {
      // check vertices
      K[] vertexMapping = embedding.getEmbedding().getVertexMapping();

      if (isValidVertex(t.getSourceId(), vertexMapping, sourceCandidate) &&
        isValidVertex(t.getTargetId(), vertexMapping, targetCandidate)) {
        edgeMapping[edgeCandidate] = edgeId;
        vertexMapping[sourceCandidate] = t.getSourceId();
        vertexMapping[targetCandidate] = t.getTargetId();

        if (hasMoreSteps()) {
          embedding.setTiePointId(vertexMapping[nextFrom]);
        }
        out.collect(embedding);
      }
    }
  }
}
