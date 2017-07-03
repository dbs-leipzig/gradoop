
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.VertexStep;

/**
 * (id, [candidates]) -> (id)
 *
 * Forwarded fields:
 *
 * f0: vertex id
 *
 * Read fields:
 *
 * f0: vertex id
 *
 * @param <K> key type
 */
@FunctionAnnotation.ForwardedFields("f0")
@FunctionAnnotation.ReadFields("f0")
public class BuildVertexStep<K> implements MapFunction<IdWithCandidates<K>, VertexStep<K>> {
  /**
   * Reduce instantiations
   */
  private final VertexStep<K> reuseTuple;

  /**
   * Constructor
   */
  public BuildVertexStep() {
    reuseTuple = new VertexStep<>();
  }

  @Override
  public VertexStep<K> map(IdWithCandidates<K> v) throws Exception {
    reuseTuple.setVertexId(v.getId());
    return reuseTuple;
  }
}
