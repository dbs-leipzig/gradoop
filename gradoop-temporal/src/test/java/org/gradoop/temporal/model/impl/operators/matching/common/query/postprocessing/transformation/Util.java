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

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.ComparableTPGMFactory;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for testing query transformations
 */
public class Util {

  /**
   * Builds a CNF from a set of lists of comparisons
   *
   * @param clauses set of lists of comparisons
   * @return CNF from clauses
   */
  public static CNF cnfFromLists(List<Comparison>... clauses) {
    List<CNFElement> cnfClauses = new ArrayList<>();
    for (List<Comparison> comparisons : clauses) {
      ArrayList<ComparisonExpression> wrappedComparisons = new ArrayList<>();
      for (Comparison comparison : comparisons) {
        wrappedComparisons.add(new ComparisonExpression(comparison, new ComparableTPGMFactory()));
      }
      cnfClauses.add(new CNFElement(wrappedComparisons));
    }
    return new CNF(cnfClauses);
  }
}
