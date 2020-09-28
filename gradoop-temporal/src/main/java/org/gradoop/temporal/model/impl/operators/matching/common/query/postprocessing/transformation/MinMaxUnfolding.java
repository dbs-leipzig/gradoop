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
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.ComparableTPGMFactory;
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
 * - a singleton clause [MAX(a,b,c) < / <= x] is reformulated to
 * [a < / <= x] AND [b < / <= x] AND [c < / <= x]
 * - a n-ary disjunctive clause [comp_1,...,comp_(i-1), MIN(a,b,c) < / <= x, comp_(i+1),...,comp_n]
 * is reformulated to
 * [comp_1,...,comp_(i-1), a < / <= x, b < / <= x, c < / <= x, comp_(i+1),...,comp_n]
 * - a n-ary disjunctive clause [comp_1,...,comp_(i-1), x < / <= MAX(a,b,c), comp_(i+1),...,comp_n]
 * is reformulated to
 * [comp_1,...,comp_(i-1), x < / <= a, x < / <= b, x < / <= c, comp_(i+1),...,comp_n]
 * - a unary clause [x < / <= MIN(a,b,c)] is reformulated to
 * [x < / <= a] AND [x < / <= b] AND [x < / <= c]
 *
 * These rules are applied exhaustively.
 * !!! This class assumes input CNFs to be normalized, i.e. not to contain > or >= !!!
 */
public class MinMaxUnfolding implements QueryTransformation {
  @Override
  public CNF transformCNF(CNF cnf) {
    if (cnf.getPredicates().size() == 0) {
      return cnf;
    } else {
      // apply transformations exhaustively
      CNF oldCNF = cnf;
      CNF newCNF = cnf.getPredicates().stream()
        .map(this::unfoldNext)
        .reduce(CNF::and).get();
      while (!newCNF.equals(oldCNF)) {
        oldCNF = newCNF;
        newCNF = newCNF.getPredicates().stream()
          .map(this::unfoldNext)
          .reduce(CNF::and).get();
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
  private CNF unfoldNext(CNFElement clause) {
    // determine whether clause is singleton or not and and trigger the corresponding rule applications
    if (clause.getPredicates().size() > 1) {
      return new CNF(
        clause.getPredicates().stream()
          // no conjunctions within a disjunctive clause
          .map(comp -> unfoldNext(comp, false))
          .reduce(CNF::or)
          .get()
      );
    } else {
      return unfoldNext(clause.getPredicates().get(0), true);
    }
  }

  /**
   * Applies the rules to a single comparison (not necessarily exhaustive), depending
   * on whether the comparison is part of a singleton clause or a n-ary one (n>1).
   *
   * @param comp              comparison to apply the rules to
   * @param allowConjunctions true iff the result is allowed to be a n-ary conjunction (n > 1)
   * @return transformed comparison as a CNF
   */
  private CNF unfoldNext(ComparisonExpression comp, boolean allowConjunctions) {
    // unwrap comparison
    ComparableExpression lhs = comp.getLhs().getWrappedComparable();
    Comparator comparator = comp.getComparator();
    ComparableExpression rhs = comp.getRhs().getWrappedComparable();

    // apply rules to comparison
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

    return new CNF(new ArrayList(
      Collections.singletonList(new CNFElement(new ArrayList(Collections.singletonList(comp))))));

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
  private CNF exists(List<TimePoint> args, Comparator comparator, ComparableExpression rhs) {
    List<ComparisonExpression> comparisons = new ArrayList<>();
    for (TimePoint arg : args) {
      comparisons.add(new ComparisonExpression(new Comparison(arg, comparator, rhs),
        new ComparableTPGMFactory()));
    }
    CNFElement singleClause = new CNFElement(comparisons);
    return new CNF(new ArrayList(Collections.singletonList(singleClause)));
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
  private CNF exists(ComparableExpression lhs, Comparator comparator, List<TimePoint> args) {
    List<ComparisonExpression> comparisons = new ArrayList<>();
    for (TimePoint arg : args) {
      comparisons.add(new ComparisonExpression(new Comparison(lhs, comparator, arg),
        new ComparableTPGMFactory()));
    }
    return new CNF(new ArrayList(Collections.singletonList(new CNFElement(comparisons))));
  }

  /**
   * Realizes a "forall" predicate as CNF.
   * E.g., {@code forAll((a,b,c), <= x)} would yield
   * a <= x AND b <= x AND c <= x
   *
   * @param args       left hand side, domain of the "forall"
   * @param comparator comparator
   * @param rhs        right hand side
   * @return "forall" predicate as CNF
   */
  private CNF forAll(List<TimePoint> args, Comparator comparator, ComparableExpression rhs) {
    List<CNFElement> clauses = new ArrayList<>();
    for (TimePoint arg : args) {
      clauses.add(new CNFElement(new ArrayList(Collections.singletonList(new ComparisonExpression(
        new Comparison(arg, comparator, rhs),
        new ComparableTPGMFactory()
      )))));
    }
    return new CNF(clauses);
  }

  /**
   * Realizes a "forall" predicate as CNF.
   * E.g., {@code forAll(x, <=, (a,b,c))} would yield
   * x <= a AND x <= b AND x <= c
   *
   * @param lhs        left hand side
   * @param comparator comparator
   * @param args       right hand side, domain of the "forall"
   * @return "forall" predicate as CNF
   */
  private CNF forAll(ComparableExpression lhs, Comparator comparator, List<TimePoint> args) {
    List<CNFElement> clauses = new ArrayList<>();
    for (TimePoint arg : args) {
      clauses.add(new CNFElement(new ArrayList(Collections.singletonList(new ComparisonExpression(
        new Comparison(lhs, comparator, arg),
        new ComparableTPGMFactory()
      )))));
    }
    return new CNF(clauses);
  }
}
