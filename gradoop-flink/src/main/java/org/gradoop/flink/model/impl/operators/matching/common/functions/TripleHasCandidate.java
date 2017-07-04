/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
