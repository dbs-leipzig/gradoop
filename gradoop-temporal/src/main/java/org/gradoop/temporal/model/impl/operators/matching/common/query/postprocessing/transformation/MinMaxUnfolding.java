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
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.time.MaxTimePoint;
import org.s1ck.gdl.model.comparables.time.MinTimePoint;
import org.s1ck.gdl.model.comparables.time.TimePoint;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.s1ck.gdl.utils.Comparator.LTE;

/**
 * Reformulates certain expressions involving MIN and MAX elements, according to the following rules:
 * - a unary clause [MAX(a,b,c) < / <= x] is reformulated to
 * [a < / <= x] AND b [< / <= x] AND [c< / <= x]
 * - a n-ary disjunctive clause [comp_1,...,comp_i, MIN(a,b,c) < / <= x, comp_(i+1),...,comp_n]
 * is reformulated to
 * [comp_1,...,comp_i, a < / <= x, b < / <= x, c < / <= x, comp_(i+1),...,comp_n]
 * - a n-ary disjunctive clause [comp_1,...,comp_i, x < / <= MAX(a,b,c), comp_(i+1),...,comp_n]
 * is reformulated to
 * [comp_1,...,comp_i, x < / <= a, x </<= b, x < / <= c, comp_(i+1),...,comp_n]
 * - a unary clause [x < / <= MIN(a,b,c)] is reformulated to
 * [x < / <= a] AND [x < / <= b] AND [x < / <= c]
 *
 * These rules are applied exhaustively.
 * !!! This class assumes input CNFs to be normalized, i.e. not to contain > or >= !!!
 */
public class MinMaxUnfolding implements QueryTransformation {
  @Override
  public TemporalCNF transformCNF(TemporalCNF cnf) {
    if (cnf.getPredicates().size() == 0) {
      return cnf;
    } else {
      TemporalCNF oldCNF = cnf;
      TemporalCNF newCNF = cnf.getPredicates().stream()
        .map(this::unfoldNext).reduce(TemporalCNF::and).get();
      while (!newCNF.equals(oldCNF)) {
        oldCNF = newCNF;
        newCNF = newCNF.getPredicates().stream()
          .map(this::unfoldNext)
          .reduce(TemporalCNF::and).get();
      }
      return newCNF;
    }
  }

  /**
   * Applies the rules to a single clause (not necessarily exhaustive, several calls may be necessary)
   *
   * @param clause clause to apply the rules to
   * @return transformed clause
   */
  private TemporalCNF unfoldNext(CNFElementTPGM clause) {
    if (clause.getPredicates().size() > 1) {
      return new TemporalCNF(
        clause.getPredicates().stream()
          .map(comp -> unfoldNext(comp, false))
          .reduce(TemporalCNF::or)
          .get()
      );
    } else {
      return unfoldNext(clause.getPredicates().get(0), true);
    }
  }

  /**
   * Applies the rules to a single comparison (not necessarily exhaustive)
   *
   * @param comp              comparison to apply the rules to
   * @param allowConjunctions true iff the result is allowed to be a n-ary CNF (n > 1)
   * @return transformed comparison
   */
  private TemporalCNF unfoldNext(ComparisonExpressionTPGM comp, boolean allowConjunctions) {
    ComparableExpression lhs = comp.getLhs().getWrappedComparable();
    Comparator comparator = comp.getComparator();
    ComparableExpression rhs = comp.getRhs().getWrappedComparable();

    if (lhs instanceof MinTimePoint) {
      if (comparator == Comparator.LT || comparator == LTE) {
        return exists(((MinTimePoint) lhs).getArgs(), comparator, rhs);
      }
    } else if (lhs instanceof MaxTimePoint) {
      if (comparator == Comparator.LT || comparator == LTE) {
        if (allowConjunctions) {
          return forAll(((MaxTimePoint) lhs).getArgs(), comparator, rhs);
        }
      }
    } else if (rhs instanceof MinTimePoint) {
      if (comparator == Comparator.LT || comparator == Comparator.LTE) {
        if (allowConjunctions) {
          return forAll(lhs, comparator, ((MinTimePoint) rhs).getArgs());
        }
      }
    } else if (rhs instanceof MaxTimePoint) {
      if (comparator == Comparator.LT || comparator == Comparator.LTE) {
        return exists(lhs, comparator, ((MaxTimePoint) rhs).getArgs());
      }
    }

    return new TemporalCNF(new ArrayList(
      Collections.singletonList(new CNFElementTPGM(new ArrayList(Collections.singletonList(comp))))));

  }

  /**
   * Realizes an "exists" predicate as CNF.
   * E.g., {@code exists((a,b,c), <=, x)} would yield
   * a <= x OR b <= x OR c <= x
   *
   * @param args       left hand side, domain of the "exists"
   * @param comparator comparator
   * @param rhs        right hand side
   * @return "exists" predicate as CNF
   */
  private TemporalCNF exists(List<TimePoint> args, Comparator comparator, ComparableExpression rhs) {
    List<ComparisonExpressionTPGM> comparisons = new ArrayList<>();
    for (TimePoint arg : args) {
      comparisons.add(new ComparisonExpressionTPGM(new Comparison(arg, comparator, rhs)));
    }
    CNFElementTPGM singleClause = new CNFElementTPGM(comparisons);
    return new TemporalCNF(new ArrayList(Collections.singletonList(singleClause)));
  }

  /**
   * Realizes an "exists" predicate as CNF.
   * E.g., {@code exists(x, <= (a,b,c)} would yield
   * x <= a OR x <= b OR x <= c
   *
   * @param lhs        left hand side
   * @param comparator comparator
   * @param args       right hand side, domain of the "exists"
   * @return "exists" predicate as CNF
   */
  private TemporalCNF exists(ComparableExpression lhs, Comparator comparator, List<TimePoint> args) {
    List<ComparisonExpressionTPGM> comparisons = new ArrayList<>();
    for (TimePoint arg : args) {
      comparisons.add(new ComparisonExpressionTPGM(new Comparison(lhs, comparator, arg)));
    }
    return new TemporalCNF(new ArrayList(Collections.singletonList(new CNFElementTPGM(comparisons))));
  }

  /**
   * Realizes a "forall" predicate as CNF.
   * E.g., {@code forAll(x, <= (a,b,c)} would yield
   * x <= a AND x <= b AND x <= c
   *
   * @param args       left hand side, domain of the "forall"
   * @param comparator comparator
   * @param rhs        right hand side
   * @return "forall" predicate as CNF
   */
  private TemporalCNF forAll(List<TimePoint> args, Comparator comparator, ComparableExpression rhs) {
    List<CNFElementTPGM> clauses = new ArrayList<>();
    for (TimePoint arg : args) {
      clauses.add(new CNFElementTPGM(new ArrayList(Collections.singletonList(new ComparisonExpressionTPGM(
        new Comparison(arg, comparator, rhs)
      )))));
    }
    return new TemporalCNF(clauses);
  }

  /**
   * Realizes a "forall" predicate as CNF.
   * E.g., {@code forAll((a,b,c), <= x)} would yield
   * a <= x AND b <= x AND c <= x
   *
   * @param lhs        left hand side
   * @param comparator comparator
   * @param args       right hand side, domain of the "forall"
   * @return "forall" predicate as CNF
   */
  private TemporalCNF forAll(ComparableExpression lhs, Comparator comparator, List<TimePoint> args) {
    List<CNFElementTPGM> clauses = new ArrayList<>();
    for (TimePoint arg : args) {
      clauses.add(new CNFElementTPGM(new ArrayList(Collections.singletonList(new ComparisonExpressionTPGM(
        new Comparison(lhs, comparator, arg)
      )))));
    }
    return new TemporalCNF(clauses);
  }
}
