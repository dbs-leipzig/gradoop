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
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.QueryTransformation;
import org.s1ck.gdl.utils.Comparator;

import java.util.stream.Collectors;

/**
 * Normalizes a {@link CNF}. Ensures that all comparisons are =, !=, < or <=
 */
public class Normalization implements QueryTransformation {

  @Override
  public CNF transformCNF(CNF cnf) {
    if (cnf.getPredicates().size() == 0) {
      return cnf;
    }
    return new CNF(
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
  private CNFElement transformDisjunction(CNFElement disj) {
    return new CNFElement(
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
  private ComparisonExpression transformComparison(ComparisonExpression comparison) {
    Comparator comparator = comparison.getComparator();
    if (comparator == Comparator.GT || comparator == Comparator.GTE) {
      return comparison.switchSides();
    } else {
      return comparison;
    }
  }


}
