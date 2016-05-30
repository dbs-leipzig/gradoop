package org.gradoop.model.impl.operators.matching.isomorphism.naive.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.tuples.EmbeddingWithTiePoint;

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

  private final Embedding reuseEmbedding;

  private final EmbeddingWithTiePoint reuseEmbeddingWithTiePoint;

  private final TraversalCode traversalCode;

  private final long vertexCount;

  private final long candidate;

  public BuildEmbeddingWithTiePoint(TraversalCode traversalCode,
    long vertexCount, long edgeCount) {
    this.traversalCode          = traversalCode;
    this.vertexCount            = vertexCount;
    this.candidate              = traversalCode.getStep(0).getFrom();
    reuseEmbedding              = new Embedding();
    reuseEmbeddingWithTiePoint = new EmbeddingWithTiePoint();
    reuseEmbedding.setEdgeEmbeddings(new GradoopId[(int) edgeCount]);
    reuseEmbeddingWithTiePoint.setEmbedding(reuseEmbedding);
  }

  @Override
  public EmbeddingWithTiePoint map(IdWithCandidates v) throws Exception {
    reuseEmbeddingWithTiePoint.setTiePointId(v.getId());
    GradoopId[] vertexEmbeddings = new GradoopId[(int) vertexCount];
    vertexEmbeddings[(int) candidate] = v.getId();
    reuseEmbedding.setVertexEmbeddings(vertexEmbeddings);
    return reuseEmbeddingWithTiePoint;
  }
}
