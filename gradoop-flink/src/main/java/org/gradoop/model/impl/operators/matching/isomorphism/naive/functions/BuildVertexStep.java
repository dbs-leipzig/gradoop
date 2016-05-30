package org.gradoop.model.impl.operators.matching.isomorphism.naive.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.tuples.VertexStep;

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
 */
@FunctionAnnotation.ForwardedFields("f0")
@FunctionAnnotation.ReadFields("f0")
public class BuildVertexStep implements MapFunction<IdWithCandidates, VertexStep> {

  private final VertexStep reuseTuple;

  public BuildVertexStep() {
    reuseTuple = new VertexStep();
  }

  @Override
  public VertexStep map(IdWithCandidates v) throws Exception {
    reuseTuple.setVertexId(v.getId());
    return reuseTuple;
  }
}
