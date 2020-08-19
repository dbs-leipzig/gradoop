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
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.utils.Comparator;

import java.util.stream.Collectors;

import static org.s1ck.gdl.utils.Comparator.LTE;

/**
 * Looks for trivial tautologies like {@code a.tx_from <= a.tx_to} or (b.prop=b.prop).
 * Every disjunctive clause containing a tautology is removed from the CNF.
 * !!! This class assumes input CNFs to be normalized, i.e. not to contain > or >= !!!
 */
public class TrivialTautologies implements QueryTransformation {
  @Override
  public CNF transformCNF(CNF cnf) {
    if (cnf.getPredicates().size() == 0) {
      return cnf;
    }
    return new CNF(
      cnf.getPredicates().stream()
        .filter(this::notTautological)
        .collect(Collectors.toList())
    );
  }

  /**
   * Checks whether a clause is not tautological, i.e. contains no tautological comparison
   *
   * @param clause clause to check for tautologies
   * @return true iff the clause is not tautological
   */
  private boolean notTautological(CNFElement clause) {
    for (ComparisonExpression comparison : clause.getPredicates()) {
      if (isTautological(comparison)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if a comparison is tautological. This check is "uninformed", it only employs
   * simple (domain) logic. The following comparisons are considered tautological:
   * - x=x
   * - x<=x
   * - a.tx_from <= a.tx_to
   * - a.val_from <= a.val_to
   * - literal1 comp literal2    iff the comparison holds
   *
   * @param comp comparison to check for tautology
   * @return true iff the comparison is tautological according to the criteria listed above.
   */
  private boolean isTautological(ComparisonExpression comp) {
    ComparableExpression lhs = comp.getLhs().getWrappedComparable();
    Comparator comparator = comp.getComparator();
    ComparableExpression rhs = comp.getRhs().getWrappedComparable();

    // x=x, x<=x
    if (lhs.equals(rhs)) {
      if (comparator.equals(Comparator.EQ) || comparator.equals(LTE)) {
        return true;
      }
    }
    // a.tx_from <= a.tx_to, a.val_from <= a.val_to
    if (lhs instanceof TimeSelector && rhs instanceof TimeSelector &&
      lhs.getVariable().equals(rhs.getVariable())) {
      if (((TimeSelector) lhs).getTimeProp().equals(TimeSelector.TimeField.TX_FROM) &&
        ((TimeSelector) rhs).getTimeProp().equals(TimeSelector.TimeField.TX_TO) ||
        ((TimeSelector) lhs).getTimeProp().equals(TimeSelector.TimeField.VAL_FROM) &&
          ((TimeSelector) rhs).getTimeProp().equals(TimeSelector.TimeField.VAL_TO)) {
        return comparator.equals(LTE);
      }
    } else if ((lhs instanceof TimeLiteral && rhs instanceof TimeLiteral) ||
      (lhs instanceof Literal && rhs instanceof Literal)) {
      // comparison of two (time) literals is tautological iff the comparison holds
      // true iff the comparison holds
      return comp.evaluate(new TemporalVertex());
    }
    return false;
  }
}
