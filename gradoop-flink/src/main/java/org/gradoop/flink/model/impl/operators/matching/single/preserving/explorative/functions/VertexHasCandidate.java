/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.functions;


import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.ExplorativePatternMatching;

/**
 * Filters vertices if their candidates contain a given candidate. This
 * method can only be applied during iteration as the candidate depends on the
 * current super step.
 *
 * Read fields:
 *
 * f1: vertex candidates
 *
 * @param <K> key type
 */
@FunctionAnnotation.ReadFields("f1")
public class VertexHasCandidate<K> extends RichFilterFunction<IdWithCandidates<K>> {
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
  public VertexHasCandidate(TraversalCode traversalCode) {
    this.traversalCode = traversalCode;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    int step = (int) getRuntimeContext()
      .getBroadcastVariable(ExplorativePatternMatching.BC_SUPERSTEP).get(0);
    candidate = (int) traversalCode.getStep(step - 1).getTo();
  }

  @Override
  public boolean filter(IdWithCandidates<K> t) throws Exception {
    return t.getCandidates()[candidate];
  }
}
