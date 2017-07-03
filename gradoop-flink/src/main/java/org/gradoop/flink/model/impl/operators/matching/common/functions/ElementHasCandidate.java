
package org.gradoop.flink.model.impl.operators.matching.common.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.matching.common.tuples
  .IdWithCandidates;

/**
 * Filters elements if their candidates contain a given candidate.
 *
 * Read fields:
 *
 * f1: candidates
 *
 * @param <K> key type
 */
@FunctionAnnotation.ReadFields("f1")
public class ElementHasCandidate<K> implements FilterFunction<IdWithCandidates<K>> {

  /**
   * Candidate to test on
   */
  private final int candidate;

  /**
   * Constructor
   *
   * @param candidate candidate to test on
   */
  public ElementHasCandidate(int candidate) {
    this.candidate = candidate;
  }

  @Override
  public boolean filter(IdWithCandidates<K> idWithCandidates) throws Exception {
    return idWithCandidates.getCandidates()[candidate];
  }
}
