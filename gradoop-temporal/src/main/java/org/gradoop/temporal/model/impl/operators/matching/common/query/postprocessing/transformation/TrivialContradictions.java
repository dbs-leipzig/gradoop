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
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.utils.Comparator;

import java.util.ArrayList;
import java.util.List;

import static org.s1ck.gdl.utils.Comparator.GT;
import static org.s1ck.gdl.utils.Comparator.LTE;

/**
 * Looks for trivial contradictions like {@code a.tx_from > a.tx_to} or (b.prop!=b.prop).
 * Every contradictory comparison is removed from its disjunctive clause.
 * If every comparison in a clause is contradictory, the whole CNF is a contradiction.
 * In this case, the transformation throws a {@link QueryContradictoryException}
 * !!! This class assumes input CNFs to be normalized, i.e. not to contain < or <= !!!
 */
public class TrivialContradictions implements QueryTransformation {

  @Override
  public CNF transformCNF(CNF cnf) throws QueryContradictoryException {
    ArrayList<CNFElement> newClauses = new ArrayList<>();
    // check every clause for contradictions
    for (CNFElement clause : cnf.getPredicates()) {
      CNFElement newClause = transformDisjunction(clause);
      newClauses.add(newClause);
    }
    return new CNF(newClauses);
  }

  /**
   * Checks a disjunctive clause for trivial contradictions and removes them.
   * If all comparisons are contradictory, an exception is thrown.
   * @param clause clause to check
   * @return the clause iff it does not contain a trivial contradiction
   * @throws QueryContradictoryException iff the clause contains a trivial contradiction
   */
  private CNFElement transformDisjunction(CNFElement clause) throws QueryContradictoryException {
    List<ComparisonExpression> oldComparisons = clause.getPredicates();
    ArrayList<ComparisonExpression> newComparisons = new ArrayList<>();
    boolean contradiction = true;
    for (ComparisonExpression comparison : oldComparisons) {
      if (!isContradictory(comparison)) {
        // contradictory comparisons are omitted in the resulting clause
        newComparisons.add(comparison);
        contradiction = false;
      }
    }
    // all comparisons contradictory?
    if (contradiction) {
      throw new QueryContradictoryException();
    } else {
      return new CNFElement(newComparisons);
    }
  }


  /**
   * checks whether a comparison is a trivial contradiction (x < x, x!=x, !(a.tx_from <= a.tx_to,
   *  contradictory comparison between two time literals)
   * @param comp comparison to check
   * @return true iff comparison is a trivial contradiction
   */
  private boolean isContradictory(ComparisonExpression comp) {
    // unwrap the comparison
    ComparableExpression lhs = comp.getLhs().getWrappedComparable();
    Comparator comparator = comp.getComparator();
    ComparableExpression rhs = comp.getRhs().getWrappedComparable();

    // x<x, x!=x
    if (lhs.equals(rhs)) {
      if (!(comparator.equals(Comparator.EQ) || comparator.equals(LTE))) {
        return true;
      }
    }
    // a.tx_from > a.tx_to
    if (lhs instanceof TimeSelector && rhs instanceof TimeSelector &&
      lhs.getVariable().equals(rhs.getVariable())) {
      if (((TimeSelector) lhs).getTimeProp().equals(TimeSelector.TimeField.TX_FROM) &&
        ((TimeSelector) rhs).getTimeProp().equals(TimeSelector.TimeField.TX_TO) ||
        ((TimeSelector) lhs).getTimeProp().equals(TimeSelector.TimeField.VAL_FROM) &&
          ((TimeSelector) rhs).getTimeProp().equals(TimeSelector.TimeField.VAL_TO)) {
        return comparator.equals(GT);
      }
    } else if ((lhs instanceof TimeLiteral && rhs instanceof TimeLiteral) ||
      (lhs instanceof Literal && rhs instanceof Literal)) {
      // comparison of two (time) literals is tautological iff the comparison holds
      // true iff the comparison holds
      return !comp.evaluate(new TemporalVertex());
    }
    return false;
  }
}
