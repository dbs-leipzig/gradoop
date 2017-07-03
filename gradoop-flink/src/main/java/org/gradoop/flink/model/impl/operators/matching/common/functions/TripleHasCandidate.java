
package org.gradoop.flink.model.impl.operators.matching.common.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;

/**
 * Filters edge triples if their candidates contain a given candidate.
 *
 * Read fields:
 *
 * f3: edge query candidates
 *
 * @param <K> key type
 */
@FunctionAnnotation.ReadFields("f3")
public class TripleHasCandidate<K> implements FilterFunction<TripleWithCandidates<K>> {
  /**
   * Candidate to test on
   */
  private final int candidate;

  /**
   * Constructor
   *
   * @param candidate candidate to test on
   */
  public TripleHasCandidate(int candidate) {
    this.candidate = candidate;
  }

  @Override
  public boolean filter(TripleWithCandidates<K> tripleWithCandidates) throws Exception {
    return tripleWithCandidates.getCandidates()[candidate];
  }
}
