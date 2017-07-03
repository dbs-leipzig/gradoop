
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.ExplorativePatternMatching;


/**
 * Filters edge triples if their candidates contain a given candidate. This
 * method can only be applied during iteration as the candidate depends on the
 * current super step.
 *
 * Read fields:
 *
 * f3: edge candidates
 *
 * @param <K> key type
 */
@FunctionAnnotation.ReadFields("f3")
public class EdgeHasCandidate<K> extends RichFilterFunction<TripleWithCandidates<K>> {

  /**
   * Traversal code
   */
  private final TraversalCode traversalCode;

  /**
   * Candidate to test on
   */
  private int candidate;

  /**
   * Constructor
   *
   * @param traversalCode traversal code to determine candidate
   */
  public EdgeHasCandidate(TraversalCode traversalCode) {
    this.traversalCode = traversalCode;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    int step = (int) getRuntimeContext()
      .getBroadcastVariable(ExplorativePatternMatching.BC_SUPERSTEP).get(0);
    candidate = (int) traversalCode.getStep(step - 1).getVia();
  }

  @Override
  public boolean filter(TripleWithCandidates<K> t) throws Exception {
    return t.getCandidates()[candidate];
  }
}
