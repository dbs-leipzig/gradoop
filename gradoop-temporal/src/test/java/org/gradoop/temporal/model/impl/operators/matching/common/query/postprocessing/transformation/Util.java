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
package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.CNFElementTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for testing query transformations
 */
public class Util {

  /**
   * Builds a TemporalCNF from a set of lists of comparisons
   *
   * @param clauses set of lists of comparisons
   * @return TemporalCNF from clauses
   */
  public static TemporalCNF cnfFromLists(List<Comparison>... clauses) {
    List<CNFElementTPGM> cnfClauses = new ArrayList<>();
    for (List<Comparison> comparisons : clauses) {
      ArrayList<ComparisonExpressionTPGM> wrappedComparisons = new ArrayList<>();
      for (Comparison comparison : comparisons) {
        wrappedComparisons.add(new ComparisonExpressionTPGM(comparison));
      }
      cnfClauses.add(new CNFElementTPGM(wrappedComparisons));
    }
    return new TemporalCNF(cnfClauses);
  }
}
