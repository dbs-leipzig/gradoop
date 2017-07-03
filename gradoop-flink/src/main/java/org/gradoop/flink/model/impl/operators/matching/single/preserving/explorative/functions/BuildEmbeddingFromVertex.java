
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.matching.common.query.Step;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EmbeddingWithTiePoint;

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
public class BuildEmbeddingFromVertex<K>
  extends BuildEmbedding<K>
  implements MapFunction<IdWithCandidates<K>, EmbeddingWithTiePoint<K>> {

  /**
   * Initial vertex candidate that determines the start of the traversal.
   */
  private final int vertexCandidate;

  /**
   * Constructor
   *
   * @param keyClazz      key type is needed for array initialization
   * @param initialStep   initial step in the traversal code
   * @param vertexCount   number of vertices in the query graph
   * @param edgeCount     number of edges in the query graph
   */
  public BuildEmbeddingFromVertex(Class<K> keyClazz, Step initialStep,
    long vertexCount, long edgeCount) {
    super(keyClazz, vertexCount, edgeCount);
    vertexCandidate = (int) initialStep.getFrom();
  }

  @Override
  public EmbeddingWithTiePoint<K> map(IdWithCandidates<K> v) throws Exception {
    reuseEmbeddingWithTiePoint.setTiePointId(v.getId());
    // candidate is same for all vertices
    reuseEmbedding.getVertexMapping()[vertexCandidate] = v.getId();
    return reuseEmbeddingWithTiePoint;
  }
}
