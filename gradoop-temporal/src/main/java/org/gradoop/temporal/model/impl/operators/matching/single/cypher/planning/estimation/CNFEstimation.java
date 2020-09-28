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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.LiteralComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.PropertySelectorComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.DurationComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.MaxTimePointComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.MinTimePointComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TemporalComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeConstantComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeLiteralComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeSelectorComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.util.ComparisonUtil;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.s1ck.gdl.model.comparables.time.Duration;
import org.s1ck.gdl.model.comparables.time.TimeConstant;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.utils.Comparator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.s1ck.gdl.utils.Comparator.EQ;
import static org.s1ck.gdl.utils.Comparator.GT;
import static org.s1ck.gdl.utils.Comparator.GTE;
import static org.s1ck.gdl.utils.Comparator.LT;
import static org.s1ck.gdl.utils.Comparator.LTE;
import static org.s1ck.gdl.utils.Comparator.NEQ;

/**
 * Class to estimate the selectivity of a CNF
 */
public class CNFEstimation {

  /**
   * Graph statistics on which the estimations are based
   */
  private final TemporalGraphStatistics stats;

  /**
   * maps variable names to their type (vertex or edge)
   */
  private Map<String, TemporalGraphStatistics.ElementType> typeMap;

  /**
   * maps variable names to labels (if known)
   */
  private Map<String, String> labelMap;

  /**
   * saves estimations so that they can be looked up, when they are checked more than once
   */
  private Map<ComparisonExpression, Double> cache;

  /**
   * Creates a new instance
   *
   * @param stats    graph statistics on which estimations are based
   * @param typeMap  map from variable name to type (vertex/edge)
   * @param labelMap map from variable name to label
   */
  public CNFEstimation(TemporalGraphStatistics stats,
                       Map<String, TemporalGraphStatistics.ElementType> typeMap,
                       Map<String, String> labelMap) {
    this.stats = stats;
    this.typeMap = typeMap;
    this.labelMap = labelMap;
    this.cache = new HashMap<>();
  }

  /**
   * Creates a new instance
   *
   * @param stats   graph statistics on which estimations are based
   * @param handler QueryHandler to create typeMap and labelMap from
   */
  public CNFEstimation(TemporalGraphStatistics stats, TemporalQueryHandler handler) {
    this.stats = stats;
    initializeWithHandler(handler);
  }

  /**
   * Initializes typeMap, labelMap and cache from a {@link TemporalQueryHandler}
   *
   * @param queryHandler QueryHandler to use for initialization
   */
  private void initializeWithHandler(TemporalQueryHandler queryHandler) {
    typeMap = new HashMap<>();
    for (String edgeVar : queryHandler.getEdgeVariables()) {
      typeMap.put(edgeVar, TemporalGraphStatistics.ElementType.EDGE);
    }
    for (String vertexVar : queryHandler.getVertexVariables()) {
      typeMap.put(vertexVar, TemporalGraphStatistics.ElementType.VERTEX);
    }

    labelMap = new HashMap<>();
    queryHandler.getLabelsForVariables(
      queryHandler.getAllVariables()).entrySet().forEach(
      entry -> {
        if (entry.getValue().length() > 0) {
          labelMap.put(entry.getKey(), entry.getValue());
        }
      });
    // pre-compute all estimations
    cache = new HashMap<>();
    for (CNFElement clause : queryHandler.getPredicates()) {
      for (ComparisonExpression comp : clause.getPredicates()) {
        cache.put(comp, estimateComparison(comp));
      }
    }
  }

  /**
   * Estimates the probability that a CNF is true, based on the graph statistics.
   * Naive assumption: all contained comparisons are pair-wise independent.
   *
   * @param cnf cnf
   * @return estimation of the probability that the CNF evaluates to true
   */
  public double estimateCNF(CNF cnf) {
    if (cnf.getPredicates().isEmpty()) {
      return 1.;
    }
    return cnf.getPredicates().stream()
      .map(this::estimateCNFElement)
      .collect(Collectors.toList())
      .stream()
      .reduce((i, j) -> i * j).get();
  }

  /**
   * Estimates the probability that a {@link CNFElement}, i.e. a disjunction of
   * comparisons, is true (based on the graph statistics).
   *
   * @param element CNFElement
   * @return estimation of probability that the CNFElement evaluates to true
   */
  private double estimateCNFElement(CNFElement element) {
    if (element.getPredicates().size() == 1) {
      return estimateComparison(element.getPredicates().get(0));
    } else {
      // recursively estimate P(a or b or c or...) as P(a) + P(b or c or...) - P(a)*P(b or c or...)
      List<ComparisonExpression> comparisons = element.getPredicates();
      double firstEst = estimateComparison(comparisons.get(0));
      CNFElement rest = new CNFElement(comparisons.subList(
        1, comparisons.size()));
      double restEst = estimateCNF(new CNF(Collections.singletonList(rest)));
      return firstEst + restEst - (firstEst * restEst);
    }
  }

  /**
   * Estimates the probability that a comparison evaluates to true, based on
   * the graph statistics.
   *
   * @param comparisonExpression comparison
   * @return estimation of the probability that the comparison evaluates to true
   */
  private double estimateComparison(ComparisonExpression comparisonExpression) {
    if (cache.containsKey(comparisonExpression)) {
      return cache.get(comparisonExpression);
    }
    double result = 1.;
    // probability of labels is not estimated here, belongs to the structural estimations
    if (isLabelComp(comparisonExpression)) {
      return 1.;
    } else {
      // more than one element in the comparison?
      if (comparisonExpression.getVariables().size() > 1) {
        result = estimateComparisonOnDifferent(comparisonExpression);
      } else {
        // exactly one element in the comparison
        result = estimateCompOnSameTPGM(comparisonExpression);
      }
    }
    cache.put(comparisonExpression, result);
    return result;
  }

  /**
   * Checks if a comparison describes the label of an element
   *
   * @param comparison comparison to check
   * @return true iff the comparison describes the label of an element
   */
  private boolean isLabelComp(ComparisonExpression comparison) {
    if (comparison.getComparator() != EQ) {
      return false;
    }
    if (comparison.getLhs() instanceof PropertySelectorComparable) {
      return ((PropertySelectorComparable) comparison.getLhs())
        .getPropertyKey().equals("__label__");
    } else if (comparison.getRhs() instanceof PropertySelectorComparable) {
      return ((PropertySelectorComparable) comparison.getRhs())
        .getPropertyKey().equals("__label__");
    }
    return false;
  }

  /**
   * Estimates the probability that a comparison involving only one element holds.
   *
   * @param comparisonExpression comparison
   * @return estimation of probability that the comparison holds
   */
  private double estimateCompOnSameTPGM(ComparisonExpression comparisonExpression) {
    QueryComparable lhs = comparisonExpression.getLhs();
    QueryComparable rhs = comparisonExpression.getRhs();

    if (ComparisonUtil.isTemporal(comparisonExpression)) {
      if (lhs instanceof TimeSelectorComparable && rhs instanceof TimeSelectorComparable) {
        // TODO implement?
        return 1.;
      } else if (lhs instanceof TimeSelectorComparable && rhs instanceof TimeLiteralComparable ||
        lhs instanceof TimeLiteralComparable && rhs instanceof TimeSelectorComparable) {
        return simpleTemporalEstimation(comparisonExpression);
      } else if (lhs instanceof MinTimePointComparable || lhs instanceof MaxTimePointComparable ||
        rhs instanceof MinTimePointComparable || rhs instanceof MaxTimePointComparable) {
        return minMaxTemporalEstimation(comparisonExpression);
      } else if ((lhs instanceof DurationComparable && rhs instanceof TimeConstantComparable) ||
        (lhs instanceof TimeConstantComparable && rhs instanceof DurationComparable)) {
        return simpleDurationComparisonEstimation(comparisonExpression);
      } else {
        return 1.;
      }
    } else {
      if ((lhs instanceof PropertySelectorComparable &&
        rhs instanceof LiteralComparable) ||
        (lhs instanceof LiteralComparable &&
        rhs instanceof PropertySelectorComparable)) {
        return simplePropertyEstimation(comparisonExpression);
      } else if (lhs instanceof PropertySelectorComparable &&
        rhs instanceof PropertySelectorComparable) {
        return complexPropertyEstimation(comparisonExpression);
      } else {
        return 1.;
      }
    }
  }

  /**
   * Estimates the probability that a comparison between a tx or val duration and
   * a time constant evaluates to true.
   * All other possible comparisons involving a duration are estimated 1.0
   *
   * @param comparisonExpression comparison
   * @return estimation of the probability that the comparison evaluates to true
   */
  private double simpleDurationComparisonEstimation(ComparisonExpression comparisonExpression) {
    QueryComparable lhs = comparisonExpression.getLhs();
    QueryComparable rhs = comparisonExpression.getRhs();
    Comparator comp = comparisonExpression.getComparator();
    // ensure that the duration is always on the left side
    if (rhs instanceof DurationComparable) {
      QueryComparable t = lhs;
      lhs = rhs;
      rhs = t;
      comp = switchComparator(comp);
    }
    if (!(rhs instanceof TimeConstantComparable)) {
      return 1.;
    }
    // only valid and tx durations can be estimated
    Duration duration = (Duration) ((DurationComparable) lhs).getWrappedComparable();
    // complex durations not supported yet, only transaction and valid interval durations
    if (!checkSimpleDuration(duration)) {
      return 1.;
    }

    String variable = new ArrayList<>(comparisonExpression.getVariables()).get(0);
    TemporalGraphStatistics.ElementType type = typeMap.get(variable);
    Optional<String> label = labelMap.containsKey(variable) ?
      Optional.of(labelMap.get(variable)) : Optional.empty();
    TimeSelector from = (TimeSelector) duration.getFrom();
    // tx or valid interval?
    boolean transaction = from.getTimeProp() == TimeSelector.TimeField.TX_FROM;
    long rhsValue = ((TimeConstant) ((TimeConstantComparable) rhs)
      .getWrappedComparable()).getMillis();

    return stats.estimateDurationProb(type, label, comp, transaction, rhsValue);
  }

  /**
   * Checks if a duration is of the simple form [a.tx_from, a.tx_to] or
   * [a.val_from, a.val_to]
   *
   * @param duration duration to check
   * @return true iff the duration has this simple form
   */
  private boolean checkSimpleDuration(Duration duration) {
    // two time selectors?
    if (!(duration.getFrom() instanceof TimeSelector) || !(duration.getTo() instanceof TimeSelector)) {
      return false;
    }
    TimeSelector from = (TimeSelector) duration.getFrom();
    TimeSelector to = (TimeSelector) duration.getTo();
    // val_from, val_to or tx_from, tx_to?
    if (!
      ((from.getTimeProp() == TimeSelector.TimeField.VAL_FROM &&
        to.getTimeProp() == TimeSelector.TimeField.VAL_TO) ||
        (from.getTimeProp() == TimeSelector.TimeField.TX_FROM &&
          to.getTimeProp() == TimeSelector.TimeField.TX_TO))) {
      return false;
    }
    // same variable for both selectors?
    return from.getVariable().equals(to.getVariable());
  }

  /**
   * Estimates the probability that a comparison involving MIN or MAX holds.
   *
   * @param comparisonExpression comparison
   * @return estimation of probability that the comparison holds
   */
  private double minMaxTemporalEstimation(ComparisonExpression comparisonExpression) {
    // TODO implement if needed
    return 1.0;
  }


  /**
   * Computes the estimation of the probability that a comparison between a time selector
   * and a time literal holds
   *
   * @param comparisonExpression comparison
   * @return estimation of the probability that the comparison holds
   */
  private double simpleTemporalEstimation(ComparisonExpression comparisonExpression) {
    TemporalComparable lhs = (TemporalComparable) comparisonExpression.getLhs();
    TemporalComparable rhs = (TemporalComparable) comparisonExpression.getRhs();
    Comparator comp = comparisonExpression.getComparator();
    // adjust the comparison so that the selector is always on the lhs
    if (rhs instanceof TimeSelectorComparable) {
      TemporalComparable t = lhs;
      lhs = rhs;
      rhs = t;
      comp = switchComparator(comp);
    }
    String variable = ((TimeSelectorComparable) lhs).getVariable();
    TemporalGraphStatistics.ElementType type = typeMap.get(variable);
    Optional<String> label = labelMap.containsKey(variable) ?
      Optional.of(labelMap.get(variable)) : Optional.empty();
    TimeSelector.TimeField field = ((TimeSelectorComparable) lhs).getTimeField();
    Long value = ((TimeLiteralComparable) rhs).getTimeLiteral().getMilliseconds();

    return stats.estimateTemporalProb(type, label, field, comp, value);
  }

  /**
   * Computes the estimation of the probability that a comparison between
   * two property selectors holds
   *
   * @param comparisonExpression comparison
   * @return estimation of the probability that the comparison holds
   */
  private double complexPropertyEstimation(ComparisonExpression comparisonExpression) {
    QueryComparable lhs = comparisonExpression.getLhs();
    QueryComparable rhs = comparisonExpression.getRhs();
    Comparator comp = comparisonExpression.getComparator();

    String variable1 = ((PropertySelectorComparable) lhs).getVariable();
    TemporalGraphStatistics.ElementType type1 = typeMap.get(variable1);
    Optional<String> label1 = labelMap.containsKey(variable1) ?
      Optional.of(labelMap.get(variable1)) : Optional.empty();
    String property1 = ((PropertySelectorComparable) lhs).getPropertyKey();

    String variable2 = ((PropertySelectorComparable) rhs).getVariable();
    TemporalGraphStatistics.ElementType type2 = typeMap.get(variable2);
    Optional<String> label2 = labelMap.containsKey(variable2) ?
      Optional.of(labelMap.get(variable2)) : Optional.empty();
    String property2 = ((PropertySelectorComparable) rhs).getPropertyKey();

    return stats.estimatePropertyProb(type1, label1, property1, comp,
      type2, label2, property2);
  }

  /**
   * Computes the estimation of the probability that a comparison between a property
   * selector and a constant holds
   *
   * @param comparisonExpression comparison
   * @return estimation of the probability that the comparison holds
   */
  private double simplePropertyEstimation(ComparisonExpression comparisonExpression) {
    QueryComparable lhs = comparisonExpression.getLhs();
    QueryComparable rhs = comparisonExpression.getRhs();
    Comparator comp = comparisonExpression.getComparator();
    // "normalize" the comparison so that the selector is on the left side
    if (rhs instanceof PropertySelectorComparable) {
      QueryComparable t = lhs;
      lhs = rhs;
      rhs = t;
      comp = switchComparator(comp);
    }
    String variable = ((PropertySelectorComparable) lhs).getVariable();
    TemporalGraphStatistics.ElementType type = typeMap.get(variable);
    Optional<String> label = labelMap.containsKey(variable) ?
      Optional.of(labelMap.get(variable)) : Optional.empty();
    String property = ((PropertySelectorComparable) lhs).getPropertyKey();
    PropertyValue value = PropertyValue.create(((LiteralComparable) rhs).getValue());
    return stats.estimatePropertyProb(type, label, property, comp, value);
  }

  /**
   * Used to switch lhs and rhs of a comparison. Switches the comparator.
   * This is not the inversion function!
   * E.g., for <, the return value is > (while the inverse of < is >=)
   *
   * @param comp comparator to switch
   * @return switched comparator
   */
  private Comparator switchComparator(Comparator comp) {
    if (comp == EQ || comp == NEQ) {
      return comp;
    } else if (comp == LT) {
      return GT;
    } else if (comp == LTE) {
      return GTE;
    } else if (comp == GT) {
      return LT;
    } else {
      return LTE;
    }
  }

  /**
   * Estimates the probability that a comparison involving more than one element holds.
   *
   * @param comparisonExpression comparison
   * @return estimation of probability that the comparison holds.
   */
  private double estimateComparisonOnDifferent(ComparisonExpression comparisonExpression) {
    QueryComparable lhs = comparisonExpression.getLhs();
    QueryComparable rhs = comparisonExpression.getRhs();

    if (ComparisonUtil.isTemporal(comparisonExpression)) {
      if (lhs instanceof TimeSelectorComparable && rhs instanceof TimeSelectorComparable) {
        return timeSelectorComparisonEstimation(comparisonExpression);
      } else if (lhs instanceof MinTimePointComparable || lhs instanceof MaxTimePointComparable ||
        rhs instanceof MinTimePointComparable || rhs instanceof MaxTimePointComparable) {
        return minMaxTemporalEstimation(comparisonExpression);
      } else if (lhs instanceof DurationComparable && rhs instanceof DurationComparable) {
        return durationComparisonEstimation(comparisonExpression);
      } else {
        return 1.;
      }
    } else {
      if (lhs instanceof PropertySelectorComparable &&
        rhs instanceof PropertySelectorComparable) {
        return complexPropertyEstimation(comparisonExpression);
      } else {
        return 1.;
      }
    }
  }

  /**
   * Computes the estimation of the probability that a comparison between two
   * durations holds
   *
   * @param comparisonExpression comparison
   * @return estimation of the probability that the comparison holds
   */
  private double durationComparisonEstimation(ComparisonExpression comparisonExpression) {
    Duration lhs = (Duration) comparisonExpression.getLhs().getWrappedComparable();
    Duration rhs = (Duration) comparisonExpression.getRhs().getWrappedComparable();
    if (!(checkSimpleDuration(lhs) && checkSimpleDuration(rhs))) {
      return 1.;
    }

    TimeSelector from1 = (TimeSelector) lhs.getFrom();
    String variable1 = from1.getVariable();
    TemporalGraphStatistics.ElementType type1 = typeMap.get(variable1);
    Optional<String> label1 = labelMap.containsKey(variable1) ?
      Optional.of(labelMap.get(variable1)) : Optional.empty();
    // transaction or valid interval
    boolean transaction1 = from1.getTimeProp() == TimeSelector.TimeField.TX_FROM;

    TimeSelector from2 = (TimeSelector) rhs.getFrom();
    String variable2 = from2.getVariable();
    TemporalGraphStatistics.ElementType type2 = typeMap.get(variable2);
    Optional<String> label2 = labelMap.containsKey(variable2) ?
      Optional.of(labelMap.get(variable2)) : Optional.empty();
    boolean transaction2 = from2.getTimeProp() == TimeSelector.TimeField.TX_FROM;

    Comparator comp = comparisonExpression.getComparator();

    return stats.estimateDurationProb(type1, label1, transaction1, comp, type2,
      label2, transaction2);
  }

  /**
   * Computes the estimation of the probability that a comparison between two time
   * selectors (of different elements) holds
   *
   * @param comparisonExpression comparison
   * @return estimation of the probability that the comparison holds
   */
  private double timeSelectorComparisonEstimation(ComparisonExpression comparisonExpression) {
    TimeSelectorComparable lhs = (TimeSelectorComparable) comparisonExpression.getLhs();
    TimeSelectorComparable rhs = (TimeSelectorComparable) comparisonExpression.getRhs();
    Comparator comp = comparisonExpression.getComparator();

    String lhsVariable = lhs.getVariable();
    TemporalGraphStatistics.ElementType type1 = typeMap.getOrDefault(lhsVariable, null);
    if (type1 == null) {
      return 0.001;
    }
    Optional<String> label1 = labelMap.containsKey(lhsVariable) ?
      Optional.of(labelMap.get(lhsVariable)) : Optional.empty();
    TimeSelector.TimeField field1 = lhs.getTimeField();

    String rhsVariable = rhs.getVariable();
    TemporalGraphStatistics.ElementType type2 = typeMap.getOrDefault(rhsVariable, null);
    if (type2 == null) {
      return 0.001;
    }
    Optional<String> label2 = labelMap.containsKey(rhsVariable) ?
      Optional.of(labelMap.get(rhsVariable)) : Optional.empty();
    TimeSelector.TimeField field2 = rhs.getTimeField();

    return stats.estimateTemporalProb(type1, label1, field1, comp, type2, label2, field2);
  }

  /**
   * Reorders the clauses of the CNF: the most selective clauses are moved to the beginning
   * of the CNF (exception: label constraint is always the first constraint).
   * Within a clause, the least selective comparisons are moved to the beginning of the clause.
   * The motivation is to minimize the number of comparisons to check when processing the CNF
   *
   * @param cnf the CNF to reorder
   * @return reordered CNF
   */
  public CNF reorderCNF(CNF cnf) {
    ArrayList<CNFElement> clauses = new ArrayList<>(cnf.getPredicates());
    // resort the clauses: labels and then selective clauses first
    clauses.sort(new java.util.Comparator<CNFElement>() {
      @Override
      public int compare(CNFElement clause1, CNFElement clause2) {
        if (clause1.getPredicates().size() == 1 && isLabelComp(clause1.getPredicates().get(0))) {
          return 100;
        } else if (clause2.getPredicates().size() == 1 &&
          isLabelComp(clause2.getPredicates().get(0))) {
          return -100;
        } else {
          return (int) (100. *
            (estimateCNFElement(clause1) - estimateCNFElement(clause2)));
        }
      }
    });

    // resort comparisons within clauses: labels and then non-selective comparisons first
    clauses = clauses.stream().map(clause -> {
      ArrayList<ComparisonExpression> comps = new ArrayList<>(
        clause.getPredicates());
      comps.sort(new java.util.Comparator<ComparisonExpression>() {
        @Override
        public int compare(ComparisonExpression c1, ComparisonExpression c2) {
          if (isLabelComp(c1)) {
            return 100;
          } else if (isLabelComp(c2)) {
            return -100;
          } else {
            return (int) (100. *
              (estimateComparison(c2) - estimateComparison(c1)));
          }
        }
      });
      return new CNFElement(comps);
    }).collect(Collectors.toCollection(ArrayList::new));

    return new CNF(clauses);
  }
}
