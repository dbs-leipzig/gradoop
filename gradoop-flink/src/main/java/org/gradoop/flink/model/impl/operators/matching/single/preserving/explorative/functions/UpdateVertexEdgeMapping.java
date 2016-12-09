package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.Step;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;


public class UpdateVertexEdgeMapping<K>
  extends UpdateMapping<K>
  implements
  FlatJoinFunction<EmbeddingWithTiePoint<K>, TripleWithCandidates<K>, EmbeddingWithTiePoint<K>> {

  private final int sourceCandidate;

  private final int edgeCandidate;

  private final int targetCandidate;

  private int nextFrom;

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

        // update tie point and collect updated embedding
        if (hasMoreSteps()) {
          embedding.setTiePointId(vertexMapping[nextFrom]);
        }
        out.collect(embedding);
      }
    }
  }
}
