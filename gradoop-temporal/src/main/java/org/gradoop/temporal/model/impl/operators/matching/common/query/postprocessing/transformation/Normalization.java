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

import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.QueryTransformation;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.CNFElementTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.util.stream.Collectors;

/**
 * Normalizes a {@link TemporalCNF}. Ensures that all comparisons are =, !=, < or <=
 */
public class Normalization implements QueryTransformation {

  @Override
  public TemporalCNF transformCNF(TemporalCNF cnf) {
    if (cnf.getPredicates().size() == 0) {
      return cnf;
    }
    return new TemporalCNF(
      cnf.getPredicates().stream()
        .map(this::transformDisjunction)
        .collect(Collectors.toList()));
  }

  /**
   * Normalize a single disjunctive clause, i.e. ensure that all comparisons are =, !=, < or <=
   *
   * @param disj clause to normalize
   * @return normalized clause
   */
  private CNFElementTPGM transformDisjunction(CNFElementTPGM disj) {
    return new CNFElementTPGM(
      disj.getPredicates().stream()
        .map(this::transformComparison)
        .collect(Collectors.toList()));
  }

  /**
   * Normalize a single comparison. If the comparison is of form a >/>= b, it is transformed to
   * b < / <= a
   *
   * @param comparison comparison to normalize
   * @return normalized comparison
   */
  private ComparisonExpressionTPGM transformComparison(ComparisonExpressionTPGM comparison) {
    Comparison comp = comparison.getGDLComparison();
    Comparator comparator = comp.getComparator();
    if (comparator == Comparator.GT || comparator == Comparator.GTE) {
      return new ComparisonExpressionTPGM(comp.switchSides());
    } else {
      return comparison;
    }
  }
}
