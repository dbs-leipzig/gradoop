package org.gradoop.model.impl.operators.matching.common.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.impl.operators.matching.common.tuples.IdWithCandidates;

/**
 * Filters elements if their candidates contain a given candidate.
 *
 * Read fields:
 *
 * f1: candidates
 */
@FunctionAnnotation.ReadFields("f1")
public class ElementHasCandidate implements FilterFunction<IdWithCandidates> {

  /**
   * Candidate to test on
   */
  private final long candidate;

  /**
   * Constructor
   *
   * @param candidate candidate to test on
   */
  public ElementHasCandidate(long candidate) {
    this.candidate = candidate;
  }

  @Override
  public boolean filter(IdWithCandidates idWithCandidates) throws Exception {
    return idWithCandidates.getCandidates().contains(candidate);
  }
}
