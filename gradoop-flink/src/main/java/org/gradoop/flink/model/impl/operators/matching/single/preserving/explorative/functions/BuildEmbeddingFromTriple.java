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
   * True, if the next step continues at the source vertex of the current step.
   * False, if the next step continues at the target vertex of the current step.
   */
  private int continueAt;
  /**
   * True, iff the initial step is a loop.
   */
  private boolean isQueryLoop;

  /**
   * Constructor
   *
   * @param keyClazz    key type is needed for array initialization
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
      continueAt = (int) traversalCode.getStep(1).getFrom();
    }
  }

  @Override
  public void flatMap(TripleWithCandidates<K> t, Collector<EmbeddingWithTiePoint<K>> out) throws
    Exception {
    if ((matchStrategy == MatchStrategy.HOMOMORPHISM) ||
      (isQueryLoop && t.getSourceId().equals(t.getTargetId())) ||
      (!isQueryLoop && !t.getSourceId().equals(t.getTargetId()))) {
        reuseEmbedding.getEdgeMapping()[edgeIndex] = t.getEdgeId();
        reuseEmbedding.getVertexMapping()[sourceIndex] = t.getSourceId();
        reuseEmbedding.getVertexMapping()[targetIndex] = t.getTargetId();
        reuseEmbeddingWithTiePoint.setTiePointId(reuseEmbedding.getVertexMapping()[continueAt]);
        out.collect(reuseEmbeddingWithTiePoint);
    }
  }
}
