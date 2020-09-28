/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation.AddTrivialConstraints;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation.BoundsInference;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation.MinMaxUnfolding;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation.Normalization;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation.SyntacticSubsumption;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation.TemporalSubsumption;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation.TrivialContradictions;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation.TrivialTautologies;

import java.util.Arrays;
import java.util.List;

/**
 * Postprocessing pipeline for CNFs.
 * Contains a list of {@link QueryTransformation} objects, that are applied to a CNF
 */
public class CNFPostProcessing {


  /**
   * List of transformations. They are applied in the order of the list
   */
  private final List<QueryTransformation> transformations;

  /**
   * Creates a new custom postprocessing pipeline
   *
   * @param transformations list of transformations to apply
   */
  public CNFPostProcessing(List<QueryTransformation> transformations) {
    this.transformations = transformations;
  }

  /**
   * Creates a default postprocessing pipeline:
   * 1. Normalization
   * 2. "Translate" suitable Min/Max expressions to simple predicates
   * 3. subsumption on a purely syntactical basis
   * 4. checking for trivial tautologies, maybe simplifying the query
   * 5. checking for trivial contradictions, maybe simplifying the query
   * 6. checking for less trivial contradictions in temporal comparisons
   * 7. inferring implicit temporal information
   * 8. subsumption with temporal information
   * 9. deduplication of clauses
   */
  public CNFPostProcessing() {
    this(Arrays.asList(
      new Normalization(),
      new MinMaxUnfolding(),
      new SyntacticSubsumption(),
      new TrivialTautologies(),
      new TrivialContradictions(),
      new AddTrivialConstraints(),
      //new CheckForCircles(),
      new BoundsInference(),
      new TemporalSubsumption(),
      new TrivialTautologies()
    ));
  }

  /**
   * Executes the postprocessing pipeline on a CNF
   *
   * @param cnf CNF to postprocess
   * @return postprocessed CNF
   * @throws QueryContradictoryException if the CNF is found to be contradictory
   *                                     during the postprocessing
   */
  public CNF postprocess(CNF cnf) throws QueryContradictoryException {
    for (int i = 0; i < transformations.size(); i++) {
      cnf = transformations.get(i).transformCNF(cnf);
    }
    return cnf;
  }

  public List<QueryTransformation> getTransformations() {
    return transformations;
  }
}
