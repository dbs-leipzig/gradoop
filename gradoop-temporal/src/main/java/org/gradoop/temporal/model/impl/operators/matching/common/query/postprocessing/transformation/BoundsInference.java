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

import org.apache.commons.lang.SerializationUtils;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.QueryTransformation;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.ComparableTPGMFactory;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.DurationComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.MaxTimePointComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.MinTimePointComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeConstantComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeLiteralComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeSelectorComparable;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.TX_FROM;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.TX_TO;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_FROM;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_TO;
import static org.s1ck.gdl.utils.Comparator.EQ;
import static org.s1ck.gdl.utils.Comparator.LTE;

/**
 * Transformation that aims to infer new, informative bounds for temporal properties
 * according to the clauses.
 * Furthermore, contradictions are detected.
 */
public class BoundsInference implements QueryTransformation {


  @Override
  public CNF transformCNF(CNF cnf) throws QueryContradictoryException {
    /*
     * stores the newly inferred lower bounds for time selectors
     */
    HashMap<TimeSelector, Long> lowerBounds = new HashMap<>();
    /*
     * stores the newly inferred upper bounds for time selectors
     */
    HashMap<TimeSelector, Long> upperBounds = new HashMap<>();
    // clauses (not) to consider
    List<CNFElement> relevantClauses = cnf.getPredicates().stream()
      .filter(this::isRelevantClause)
      .collect(Collectors.toList());
    List<CNFElement> otherClauses = cnf.getPredicates().stream()
      .filter(clause -> !isRelevantClause(clause))
      .collect(Collectors.toList());
    List<ComparisonExpression> relevantComparisons = relevantClauses.stream()
      .map(clause -> clause.getPredicates().get(0))
      .collect(Collectors.toList());

    // stores all tuples (a,b) with a=b in relevant comparisons
    HashMap<ComparableExpression, HashSet<ComparableExpression>> cEq = new HashMap<>();
    // stores all tuples (a,b) with a<=b in relevant comparisons
    HashMap<ComparableExpression, HashSet<ComparableExpression>> cLeq = new HashMap<>();
    // stores all tuples (a,b) with a<b in relevant comparisons
    HashMap<ComparableExpression, HashSet<ComparableExpression>> cLt = new HashMap<>();
    // stores all tuples (a,b) with a<b in relevant comparisons
    HashMap<ComparableExpression, HashSet<ComparableExpression>> cNeq = new HashMap<>();


    // init bounds
    for (ComparisonExpression comp: relevantComparisons) {
      ComparableExpression lhs = comp.getLhs().getWrappedComparable();
      ComparableExpression rhs = comp.getRhs().getWrappedComparable();
      Comparator comparator = comp.getComparator();
      if (comp.getLhs() instanceof TimeSelectorComparable) {
        lowerBounds.put((TimeSelector) lhs, Long.MIN_VALUE);
        upperBounds.put((TimeSelector) lhs, Long.MAX_VALUE);
      }
      if (comp.getRhs() instanceof TimeSelectorComparable) {
        lowerBounds.put((TimeSelector) rhs, Long.MIN_VALUE);
        upperBounds.put((TimeSelector) rhs, Long.MAX_VALUE);
      }
      // init c-relations
      if (comparator.equals(EQ)) {
        cEq.putIfAbsent(lhs, new HashSet<>());
        cEq.putIfAbsent(rhs, new HashSet<>());
        cEq.get(lhs).add(rhs);
        cEq.get(rhs).add(lhs);
      } else if (comparator.equals(Comparator.LTE)) {
        cLeq.putIfAbsent(lhs, new HashSet<>());
        cLeq.get(lhs).add(rhs);
      } else if (comparator.equals(Comparator.LT)) {
        cLt.putIfAbsent(lhs, new HashSet<>());
        cLt.get(lhs).add(rhs);
      } else if (comparator.equals(Comparator.NEQ)) {
        cNeq.putIfAbsent(lhs, new HashSet<>());
        cNeq.get(lhs).add(rhs);
      }


    }

    // compute closures for =,<,<=
    List<HashMap<ComparableExpression, HashSet<ComparableExpression>>> closures =
      computeClosureRelations(cEq, cLeq, cLt);

    checkLtContradictions(closures.get(2));
    // infer new bounds
    List<HashMap<TimeSelector, Long>> newBounds = updateEq(closures.get(0), lowerBounds, upperBounds);
    newBounds = updateLeq(closures.get(1), newBounds.get(0), newBounds.get(1));
    newBounds = updateLt(closures.get(2), newBounds.get(0), newBounds.get(1));

    // check them for contradictions with NEQ clauses
    checkNeqContradictions(cNeq, newBounds.get(0), newBounds.get(1));
    // filter redundant clauses like a.tx_from <= a.tx_to and comparisons between selectors and literals
    List<ComparisonExpression> remainingOldComparisons = filterRedundantComparisons(relevantComparisons);
    // construct new clauses
    List<ComparisonExpression> newComparisons = comparisons(newBounds.get(0), newBounds.get(1));

    // construct the CNF
    List<CNFElement> remainingOldClauses = remainingOldComparisons.stream()
      .map(comparison -> new CNFElement(Arrays.asList(comparison)))
      .collect(Collectors.toList());
    List<CNFElement> newClauses = newComparisons.stream()
      .map(comparison -> new CNFElement(Arrays.asList(comparison)))
      .collect(Collectors.toList());

    List<CNFElement> allClauses = otherClauses;
    allClauses.addAll(remainingOldClauses);
    allClauses.addAll(newClauses);
    return new CNF(allClauses);
  }

  /**
   * Constructs new comparisons from the inferred bounds
   * @param lowerBounds inferred lower bounds
   * @param upperBounds inferred upper bounds
   * @return inferred comparisons
   */
  private List<ComparisonExpression> comparisons(HashMap<TimeSelector, Long> lowerBounds,
                                                 HashMap<TimeSelector, Long> upperBounds) {
    List<ComparisonExpression> newComparisons = new ArrayList<>();
    // for all selectors, as lowerBounds has the same keys as upperBounds
    for (Map.Entry<TimeSelector, Long> entry : lowerBounds.entrySet()) {
      TimeSelector selector = entry.getKey();
      Long lower = entry.getValue();
      Long upper = upperBounds.get(selector);
      if (lower.equals(upper)) {
        newComparisons.add(new ComparisonExpression(
          new Comparison(selector, EQ, new TimeLiteral(lower)),
          new ComparableTPGMFactory()));
      } else {
        if (lower > Long.MIN_VALUE) {
          // check if informative: lower bound of from is trivial lower bound of to
          if (selector.getTimeProp().equals(TX_TO)) {
            TimeSelector txFromSel = new TimeSelector(selector.getVariable(), TX_FROM);
            if (lowerBounds.getOrDefault(txFromSel, Long.MIN_VALUE).equals(lower)) {
              continue;
            }
          } else if (selector.getTimeProp().equals(VAL_TO)) {
            TimeSelector valFromSel = new TimeSelector(selector.getVariable(), VAL_FROM);
            if (lowerBounds.getOrDefault(valFromSel, Long.MIN_VALUE).equals(lower)) {
              continue;
            }
          }
          // informative => build comparison
          newComparisons.add(new ComparisonExpression(
            new Comparison(new TimeLiteral(lower), LTE, selector),
            new ComparableTPGMFactory()));
        }
        if (upper < Long.MAX_VALUE) {
          // analagously as for lower bounds
          // upper bound of to is trivial upper of from
          if (selector.getTimeProp().equals(TimeSelector.TimeField.TX_FROM)) {
            TimeSelector txToSel = new TimeSelector(selector.getVariable(), TX_TO);
            if (upperBounds.getOrDefault(txToSel, Long.MAX_VALUE).equals(upper)) {
              continue;
            }
          } else if (selector.getTimeProp().equals(TimeSelector.TimeField.VAL_FROM)) {
            TimeSelector valToSel = new TimeSelector(selector.getVariable(), VAL_TO);
            if (upperBounds.getOrDefault(valToSel, Long.MAX_VALUE).equals(upper)) {
              continue;
            }
          }
          // informative => build comparison
          newComparisons.add(new ComparisonExpression(
            new Comparison(selector, LTE, new TimeLiteral(upper)),
            new ComparableTPGMFactory()));
        }
      }
    }
    return newComparisons;
  }


  /**
   * Checks a set of comparisons for trivial comparisons like a.tx_from <= a.tx_to or
   * l1 < l2 (l1, l2 literals).
   * Furthermore, comparisons between selectors and literals are removed, as they are
   * replaced by those gained from the bounds
   * @param comparisons comparisons to filter
   * @return @code{comparisons} without redundant comparisons
   */
  private List<ComparisonExpression> filterRedundantComparisons(List<ComparisonExpression> comparisons) {
    List<ComparisonExpression> newComparisons = new ArrayList<>();

    // filter out redundant clauses
    for (ComparisonExpression comp: comparisons) {
      QueryComparable rhs = comp.getRhs();
      QueryComparable lhs = comp.getLhs();
      Comparator comparator = comp.getComparator();
      // a.tx_from <= a.tx_to redundant
      if (rhs instanceof TimeSelectorComparable && lhs instanceof TimeSelectorComparable &&
        comparator.equals(Comparator.LTE)) {
        if (((TimeSelectorComparable) rhs).getVariable()
          .equals(((TimeSelectorComparable) lhs).getVariable())) {
          if ((((TimeSelectorComparable) lhs).getTimeField().equals(VAL_FROM) &&
            ((TimeSelectorComparable) rhs).getTimeField().equals(VAL_TO)) ||
            (((TimeSelectorComparable) lhs).getTimeField().equals(TX_FROM) &&
              ((TimeSelectorComparable) rhs).getTimeField().equals(TX_TO))) {
            continue;
          }
        } else {
          newComparisons.add(comp);
        }
      } else if (rhs instanceof TimeLiteralComparable && lhs instanceof TimeLiteralComparable) {
        // comparisons between literals not informative.
        // Contradictory comparisons have been filtered out before
        // cf. @link{TrivialContradictions}
        continue;
      } else if ((rhs instanceof TimeLiteralComparable && lhs instanceof TimeSelectorComparable) ||
        (rhs instanceof TimeSelectorComparable && lhs instanceof TimeLiteralComparable)) {
        // comparisons between selectors and literals are redundant,
        // as they are contained in the inferred bounds
        continue;
      } else {
        newComparisons.add(comp);
      }
    }
    return newComparisons;
  }

  /**
   * Checks for contradictions between the inferred bounds and all relevant != constraints
   * @param cNeq != constraints as relations
   * @param lowerBounds inferred lower bounds
   * @param upperBounds inferred upper bounds
   * @throws QueryContradictoryException if a contradiction is encountered
   */
  private void checkNeqContradictions(HashMap<ComparableExpression, HashSet<ComparableExpression>> cNeq,
                                      HashMap<TimeSelector, Long> lowerBounds,
                                      HashMap<TimeSelector, Long> upperBounds)
    throws QueryContradictoryException {
    ArrayList<ArrayList<ComparableExpression>> neq = relationToTuples(cNeq);
    for (ArrayList<ComparableExpression> tuple : neq) {
      if (tuple.get(0) instanceof TimeSelector) {
        TimeSelector sel = (TimeSelector) tuple.get(0);
        // if lower(sel)!=upper(sel), no constraint sel!=x is contradictory
        if (!lowerBounds.get(sel).equals(upperBounds.get(sel))) {
          return;
        }
        // selector1 != selector2 only contradictory if both selectors have the same fixed value
        // (lower(selector1)=upper(selector1)=lower(selector2)=upper(selector2)
        if (tuple.get(1) instanceof TimeSelector) {
          TimeSelector sel2 = (TimeSelector) tuple.get(1);
          if (lowerBounds.get(sel).equals(lowerBounds.get(sel2)) &&
            lowerBounds.get(sel2).equals(upperBounds.get(sel2))) {
            throw new QueryContradictoryException();
          } // selector != literal only contradictory if lower(selector)=upper(selector)=literal
        } else if (tuple.get(1) instanceof TimeLiteral) {
          Long literalValue = ((TimeLiteral) tuple.get(1)).getMilliseconds();
          if (lowerBounds.get(sel).equals(literalValue)) {
            throw new QueryContradictoryException();
          }
        }
      } else if (tuple.get(0) instanceof TimeLiteral && tuple.get(1) instanceof TimeSelector) {
        // selector != literal only contradictory if lower(selector)=upper(selector)=literal
        TimeSelector sel = (TimeSelector) tuple.get(1);
        Long literalValue = ((TimeLiteral) tuple.get(0)).getMilliseconds();
        if (lowerBounds.get(sel).equals(upperBounds.get(sel)) && lowerBounds.get(sel).equals(literalValue)) {
          throw new QueryContradictoryException();
        }
      }
    }
  }

  /**
   * Updates lower and upper bounds with the lower-than relations
   * @param rLt lower-than relations
   * @param lowerBounds lower bounds
   * @param upperBounds upper bounds
   * @return updated lower and upper bounds as a list [lower, upper]
   * @throws QueryContradictoryException if a contradiction is encountered
   */
  private List<HashMap<TimeSelector, Long>> updateLt(
    HashMap<ComparableExpression, HashSet<ComparableExpression>> rLt,
    HashMap<TimeSelector, Long> lowerBounds, HashMap<TimeSelector, Long> upperBounds)
    throws QueryContradictoryException {

    ArrayList<ArrayList<ComparableExpression>> lt = relationToTuples(rLt);
    for (ArrayList<ComparableExpression> tuple : lt) {
      TimeSelector selector = null;
      Long literalValue = null;
      // only comparisons between a selector and a literal are interesting here
      // selector < literal  => upper(selector) = min(upper(selector), literal-1)
      if (tuple.get(0) instanceof TimeSelector && tuple.get(1) instanceof TimeLiteral) {
        selector = (TimeSelector) tuple.get(0);
        literalValue = ((TimeLiteral) tuple.get(1)).getMilliseconds() - 1;
        upperBounds.put(selector, Math.min(upperBounds.get(selector), literalValue));
      } else if (tuple.get(1) instanceof TimeSelector &&
        tuple.get(0) instanceof TimeLiteral) {
        // selector > literal  => lower(selector) = max(lower(selector), literal+1)
        selector = (TimeSelector) tuple.get(1);
        literalValue = ((TimeLiteral) tuple.get(0)).getMilliseconds() + 1;
        lowerBounds.put(selector, Math.max(lowerBounds.get(selector), literalValue));
      } else {
        continue;
      }

      if (lowerBounds.get(selector) > upperBounds.get(selector)) {
        throw new QueryContradictoryException();
      }
    }

    return Arrays.asList(lowerBounds, upperBounds);
  }


  /**
   * Updates lower and upper bounds with the lighter-or-equal relations
   * @param rLeq leq relations
   * @param lowerBounds lower bounds
   * @param upperBounds upper bounds
   * @return updated lower and upper bounds as a list [lower, upper]
   * @throws QueryContradictoryException if a contradiction is encountered
   */
  private List<HashMap<TimeSelector, Long>> updateLeq(
    HashMap<ComparableExpression, HashSet<ComparableExpression>> rLeq,
    HashMap<TimeSelector, Long> lowerBounds, HashMap<TimeSelector, Long> upperBounds)
    throws QueryContradictoryException {

    ArrayList<ArrayList<ComparableExpression>> leq = relationToTuples(rLeq);
    for (ArrayList<ComparableExpression> tuple : leq) {
      TimeSelector selector = null;
      Long literalValue = null;
      // only comparisons between a selector and a literal are interesting here
      // selector <= literal  => upper(selector) = min(upper(selector), literal)
      if (tuple.get(0) instanceof TimeSelector && tuple.get(1) instanceof TimeLiteral) {
        selector = (TimeSelector) tuple.get(0);
        literalValue = ((TimeLiteral) tuple.get(1)).getMilliseconds();
        upperBounds.put(selector, Math.min(upperBounds.get(selector), literalValue));
      } else if (tuple.get(1) instanceof TimeSelector &&
        tuple.get(0) instanceof TimeLiteral) {
        // selector >= literal  => lower(selector) = max(lower(selector), literal)
        selector = (TimeSelector) tuple.get(1);
        literalValue = ((TimeLiteral) tuple.get(0)).getMilliseconds();
        lowerBounds.put(selector, Math.max(lowerBounds.get(selector), literalValue));
      } else {
        continue;
      }

      if (lowerBounds.get(selector) > upperBounds.get(selector)) {
        throw new QueryContradictoryException();
      }
    }

    return Arrays.asList(lowerBounds, upperBounds);
  }

  /**
   * Updates lower and upper bounds with the equality relations
   * @param rEq equality relations
   * @param lowerBounds lower bounds
   * @param upperBounds upper bounds
   * @return updated lower and upper bounds as a list [lower, upper]
   * @throws QueryContradictoryException if a contradiction is encountered
   */
  private List<HashMap<TimeSelector, Long>> updateEq(
    HashMap<ComparableExpression, HashSet<ComparableExpression>> rEq,
    HashMap<TimeSelector, Long> lowerBounds, HashMap<TimeSelector, Long> upperBounds)
    throws QueryContradictoryException {

    ArrayList<ArrayList<ComparableExpression>> eq = relationToTuples(rEq);
    for (ArrayList<ComparableExpression> tuple : eq) {
      TimeSelector selector = null;
      TimeLiteral literal = null;
      // only comparisons between a selector and a literal are interesting here
      if (tuple.get(0) instanceof TimeSelector && tuple.get(1) instanceof TimeLiteral) {
        selector = (TimeSelector) tuple.get(0);
        literal = (TimeLiteral) tuple.get(1);
      } else if (tuple.get(1) instanceof TimeSelector && tuple.get(0) instanceof TimeLiteral) {
        selector = (TimeSelector) tuple.get(1);
        literal = (TimeLiteral) tuple.get(0);
      } else {
        continue;
      }
      // for selector=literal, lowerBound(selector) = literal and upperBound(selector) = literal
      Long literalValue = literal.getMilliseconds();
      if (lowerBounds.get(selector) > literalValue || upperBounds.get(selector) < literalValue) {
        throw new QueryContradictoryException();
      } else {
        lowerBounds.put(selector, literalValue);
        upperBounds.put(selector, literalValue);
      }
    }
    return Arrays.asList(lowerBounds, upperBounds);
  }

  /**
   * Flattens the map representation of a relation to a list of list of ComparableExpressions.
   * Each list has the length 2, representing one tuple in the relation.
   * @param rel relation to flatten
   * @return flattened relation
   */
  private ArrayList<ArrayList<ComparableExpression>> relationToTuples(HashMap<ComparableExpression,
    HashSet<ComparableExpression>> rel) {
    ArrayList<ArrayList<ComparableExpression>> list = new ArrayList<>();
    for (Map.Entry<ComparableExpression, HashSet<ComparableExpression>> mapEntry: rel.entrySet()) {
      ComparableExpression c1 = mapEntry.getKey();
      for (ComparableExpression c2 : mapEntry.getValue()) {
        list.add(new ArrayList<>(Arrays.asList(c1, c2)));
      }
    }
    return list;
  }

  /**
   * Checks the lighter-than relations for tuples (a,a) and throws an exception if such a tuple is
   * encountered. Other contradictions in this relation are checked later
   * @param ltRelation lighter-than relation
   * @throws QueryContradictoryException if a tuple (a,a) is encountered
   */
  private void checkLtContradictions(HashMap<ComparableExpression, HashSet<ComparableExpression>> ltRelation)
    throws QueryContradictoryException {
    for (Map.Entry<ComparableExpression, HashSet<ComparableExpression>> tuple : ltRelation.entrySet()) {
      ComparableExpression key = tuple.getKey();
      for (ComparableExpression value : tuple.getValue()) {
        if (key.equals(value)) {
          throw new QueryContradictoryException();
        }
      }
    }
  }

  /**
   * Computes relations rEq, rLeq, rLt so that
   * rEq contains (a,b) iff relevant comparisons imply a=b
   * rLeq contains (a,b) iff relevant comparisons imply a<=b, but not a=b or a < b
   * rLt contains (a,b) iff relevant comparisons imply a < b
   * @param cEq relation containing (a,b) iff relevant comparisons contain a=b
   * @param cLeq relation containing (a,b) iff relevant comparisons contain a<=b
   * @param cLt relation containing (a,b) iff relevant comparisons contain a < b
   * @return list [rEq, rLeq, rLt]
   */
  private List<HashMap<ComparableExpression, HashSet<ComparableExpression>>> computeClosureRelations(
    HashMap<ComparableExpression, HashSet<ComparableExpression>> cEq,
    HashMap<ComparableExpression, HashSet<ComparableExpression>> cLeq,
    HashMap<ComparableExpression, HashSet<ComparableExpression>> cLt) {
    // init relations
    HashMap<ComparableExpression, HashSet<ComparableExpression>> rEq;
    HashMap<ComparableExpression, HashSet<ComparableExpression>> rLeq;
    HashMap<ComparableExpression, HashSet<ComparableExpression>> rLt;
    // compute them
    rEq = union(transitiveClosure(cEq), selfInverses(transitiveClosure(union(cEq, cLeq))));
    rLeq = subtract(transitiveClosure(union(rEq, cLeq)), rEq);
    rLt = transitiveClosure(union(
      composition(union(rEq, rLeq), cLt),
      composition(cLt, union(rEq, rLeq)),
      cLt
    ));
    return Arrays.asList(rEq, rLeq, rLt);
  }

  /**
   * Returns a subrelation containing all tuples whose inverses are also contained in the relation
   * I.e., a tuple (a,b) is included in the return value, if (a,b) in rel and (b,a) in rel.
   * @param rel relation
   * @return subrelation containing all tuples whose inverses are also contained @code{rel}
   */
  private HashMap<ComparableExpression, HashSet<ComparableExpression>> selfInverses(
    HashMap<ComparableExpression, HashSet<ComparableExpression>> rel) {

    HashMap<ComparableExpression, HashSet<ComparableExpression>> newRel = new HashMap<>();
    for (Map.Entry<ComparableExpression, HashSet<ComparableExpression>> r: rel.entrySet()) {
      ComparableExpression key = r.getKey();
      for (ComparableExpression v: r.getValue()) {
        if (rel.containsKey(v)) {
          if (rel.get(v).contains(key)) {
            newRel.putIfAbsent(key, new HashSet<>());
            newRel.get(key).add(v);
          }
        }
      }
    }

    return newRel;
  }

  /**
   * Subtracts one relation from another
   * @param rel1 relation to subtract from
   * @param rel2 relation to be subtracted
   * @return @code{rel1} - @code{rel2}
   */
  private HashMap<ComparableExpression, HashSet<ComparableExpression>> subtract(
    HashMap<ComparableExpression, HashSet<ComparableExpression>> rel1,
    HashMap<ComparableExpression, HashSet<ComparableExpression>> rel2) {

    HashMap<ComparableExpression, HashSet<ComparableExpression>> newRel = new HashMap<>();

    for (Map.Entry<ComparableExpression, HashSet<ComparableExpression>> r1: rel1.entrySet()) {
      ComparableExpression key = r1.getKey();
      HashSet<ComparableExpression> values1 = r1.getValue();
      HashSet<ComparableExpression> values2 = rel2.getOrDefault(key, new HashSet<>());
      HashSet<ComparableExpression> newValues = new HashSet<>(values1);
      newValues.removeAll(values2);
      newRel.put(key, new HashSet<>(newValues));
    }
    return newRel;
  }



  /**
   * Compute the transitive closure of a relation
   * @param rel relation
   * @return transitive closure of relation
   */
  private HashMap<ComparableExpression, HashSet<ComparableExpression>> transitiveClosure(
    HashMap<ComparableExpression, HashSet<ComparableExpression>> rel) {

    HashMap<ComparableExpression, HashSet<ComparableExpression>> newRel = rel;
    boolean changed = true;
    // apply transitivity until closure is complete: (a,b) in rel, (b,c) in rel => (a,c) in rel
    while (changed) {
      HashMap<ComparableExpression, HashSet<ComparableExpression>> oldRel =
        (HashMap<ComparableExpression, HashSet<ComparableExpression>>) SerializationUtils.clone(newRel);
      changed = false;
      for (Map.Entry<ComparableExpression, HashSet<ComparableExpression>> r: oldRel.entrySet()) {
        ComparableExpression key = r.getKey();
        for (ComparableExpression value: r.getValue()) {
          newRel.putIfAbsent(key, new HashSet<>());
          changed = newRel.get(key).add(value) || changed;
          if (oldRel.containsKey(value)) {
            changed = newRel.get(key).addAll(oldRel.get(value)) || changed;
          }
        }
      }
    }
    return newRel;
  }

  /**
   * Composition of two relations
   * @param rel1 first relation
   * @param rel2 second relation
   * @return composition of both
   */
  private HashMap<ComparableExpression, HashSet<ComparableExpression>> composition(
    HashMap<ComparableExpression, HashSet<ComparableExpression>> rel1,
    HashMap<ComparableExpression, HashSet<ComparableExpression>> rel2) {

    HashMap<ComparableExpression, HashSet<ComparableExpression>> newRel = new HashMap<>();
    // (a,b) in rel1, (b,c) in rel2 => put (a,c) into newRel
    for (Map.Entry<ComparableExpression, HashSet<ComparableExpression>> r1: rel1.entrySet()) {
      ComparableExpression key = r1.getKey();
      for (ComparableExpression oldValue: r1.getValue()) {
        // transitive relation?
        if (rel2.containsKey(oldValue)) {
          HashSet<ComparableExpression> newValues = rel2.get(oldValue);
          newRel.putIfAbsent(key, new HashSet<>());
          newRel.get(key).addAll(newValues);
        }
      }
    }
    return newRel;
  }

  /**
   * Unites relations
   * @param relations relations to unite
   * @return united relation
   */
  private HashMap<ComparableExpression, HashSet<ComparableExpression>> union(
    HashMap<ComparableExpression, HashSet<ComparableExpression>>... relations) {
    HashMap<ComparableExpression, HashSet<ComparableExpression>> newRel = new HashMap<>();
    // add all relations from the array to newRel
    for (HashMap<ComparableExpression, HashSet<ComparableExpression>> relation : relations) {
      for (Map.Entry<ComparableExpression, HashSet<ComparableExpression>> entry: relation.entrySet()) {
        ComparableExpression key = entry.getKey();
        HashSet<ComparableExpression> values = entry.getValue();
        newRel.putIfAbsent(key, new HashSet<>());
        newRel.get(key).addAll(values);
      }
    }
    return newRel;
  }

  /**
   * Checks whether a clause is relevant, i.e. it has size 1 and does not contain
   * MIN/MAX or duration
   * @param clause clause to check
   * @return true iff clause is relevant
   */
  private boolean isRelevantClause(CNFElement clause) {
    return clause.size() == 1 && !isMinMax(clause.getPredicates().get(0)) &&
      !isDuration(clause.getPredicates().get(0));
  }

  /**
   * Checks whether a given wrapped comparison contains a MIN or MAX expression
   * @param comp comparison expression to check for MIN/MAX
   * @return true iff comp contains MIN or MAX
   */
  private boolean isMinMax(ComparisonExpression comp) {
    return comp.getLhs() instanceof MinTimePointComparable ||
      comp.getLhs() instanceof MaxTimePointComparable ||
      comp.getRhs() instanceof MinTimePointComparable ||
      comp.getRhs() instanceof MaxTimePointComparable;
  }

  /**
   * Checks if a comparison contains a duration
   * @param comp comparison
   * @return true iff comp contains a duration
   */
  private boolean isDuration(ComparisonExpression comp) {
    return comp.getLhs() instanceof DurationComparable ||
      comp.getRhs() instanceof DurationComparable ||
      comp.getLhs() instanceof TimeConstantComparable ||
      comp.getRhs() instanceof TimeConstantComparable;
  }
}
