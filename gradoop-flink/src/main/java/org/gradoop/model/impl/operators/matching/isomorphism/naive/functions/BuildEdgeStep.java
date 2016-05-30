package org.gradoop.model.impl.operators.matching.isomorphism.naive.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.tuples.EdgeStep;


import org.gradoop.model.impl.operators.matching.isomorphism.naive.utils
  .Constants;

/**
 * Converts an edge into a step edge according to the traversal.
 *
 * Forwarded fields:
 *
 * f0: edge id
 */
@FunctionAnnotation.ForwardedFields("f0")
public class BuildEdgeStep
  extends RichMapFunction<TripleWithCandidates, EdgeStep> {

  /**
   * Reduce instantiations
   */
  private final EdgeStep reuseTuple;

  /**
   * Traversal code to determine correct step.
   */
  private final TraversalCode traversalCode;

  /**
   * True, if edge is outgoing
   */
  private boolean isOutgoing;

  /**
   * Constructor
   *
   * @param traversalCode traversal code
   */
  public BuildEdgeStep(TraversalCode traversalCode) {
    this.traversalCode = traversalCode;
    reuseTuple = new EdgeStep();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    int step = (int) getRuntimeContext()
      .getBroadcastVariable(Constants.BC_SUPERSTEP).get(0);
    isOutgoing = traversalCode.getStep(step - 1).isOutgoing();
  }

  @Override
  public EdgeStep map(TripleWithCandidates t) throws Exception {
    reuseTuple.setEdgeId(t.getEdgeId());
    reuseTuple.setTiePointId(isOutgoing ? t.getSourceId() : t.getTargetId());
    reuseTuple.setNextId(isOutgoing ? t.getTargetId() : t.getSourceId());
    return reuseTuple;
  }
}
