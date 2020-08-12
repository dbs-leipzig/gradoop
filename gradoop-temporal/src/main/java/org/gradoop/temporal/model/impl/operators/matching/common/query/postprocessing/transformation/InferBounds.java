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

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.QueryTransformation;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.CNFElementTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.QueryComparableTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.MaxTimePointComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.MinTimePointComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.s1ck.gdl.utils.Comparator.EQ;
import static org.s1ck.gdl.utils.Comparator.GT;
import static org.s1ck.gdl.utils.Comparator.GTE;
import static org.s1ck.gdl.utils.Comparator.LT;
import static org.s1ck.gdl.utils.Comparator.LTE;
import static org.s1ck.gdl.utils.Comparator.NEQ;

/**
 * Rewrites the query to an equivalent one, aiming to speed up its processing.
 * The class attempts to make implicit bounds for query variables explicit.
 * E.g., a query {@code a.tx_to < b.tx_to AND b.tx_to <= 2020-05-01} implies
 * {@code a.tx_to <= 2020-05-01}. This clause would be added to the query by this class.
 * There might be duplicate clauses after this operation.
 * !!!This class assumes the CNF to be normalized, i.e. not to contain > or >= !!!
 * Furthermore, it is assumed that a {@link CheckForCircles} is applied to the CNF
 * before it is processed here.
 * , an infinite loop might occur.
 */
public class InferBounds implements QueryTransformation {



  /**
   * The processed query
   */
  private TemporalCNF query;

  /**
   * All comparisons that make up a single clause in the CNF
   */
  private List<ComparisonExpressionTPGM> relevantComparisons;

  /**
   * All variables in relevant comparisons
   */
  private Set<String> relevantVariables;

  /**
   * Assigns every query variable for every time field (tx_from, tx_to, val_from, val_to)
   * a lower bound and an upper bound
   */
  private HashMap<String, HashMap<TimeSelector.TimeField, Long[]>> bounds;

  /**
   * stores the explicit bounds that are stated in the CNF
   */
  private HashMap<String, HashMap<TimeSelector.TimeField, Long[]>> initialBounds;


  /**
   * Initializes the bounds for each variable.
   * The initial lower bound of every time field (tx_from, tx_to, val_from, val_to)
   * is {@code Long.MIN_VALUE}, the upper bound {@code Long.MAX_VALUE}
   */
  private void initBounds() {
    bounds = new HashMap<>();
    Long[] init = new Long[] {Long.MIN_VALUE, Long.MAX_VALUE};
    for (String variable : getRelevantVariables()) {
      bounds.put(variable, new HashMap<>());
      bounds.get(variable).put(TimeSelector.TimeField.TX_FROM, init);
      bounds.get(variable).put(TimeSelector.TimeField.TX_TO, init);
      bounds.get(variable).put(TimeSelector.TimeField.VAL_FROM, init);
      bounds.get(variable).put(TimeSelector.TimeField.VAL_TO, init);
    }
    initialBounds = new HashMap<>(bounds);
  }

  /**
   * Returns all temporal comparisons that must hold in the query, i.e. all temporal comparisons
   * that make up a singleton clause in the CNF
   *
   * @return list of all necessary temporal comparisons in the CNF
   */
  private List<ComparisonExpressionTPGM> getRelevantComparisons() {
    return query.getPredicates().stream()
      .filter(clause -> clause.size() == 1 &&
        clause.getPredicates().get(0).isTemporal() &&
        !isMinMax(clause.getPredicates().get(0)))
      .map(clause -> clause.getPredicates().get(0))
      .collect(Collectors.toList());
  }

  /**
   * Checks whether a comparison involves a MIN or MAX timestamp
   * @param comparisonExpressionTPGM comparison to check
   * @return true iff the comparison involves a MIN or MAX timestamp
   */
  private boolean isMinMax(ComparisonExpressionTPGM comparisonExpressionTPGM) {
    QueryComparableTPGM lhs = comparisonExpressionTPGM.getLhs();
    QueryComparableTPGM rhs = comparisonExpressionTPGM.getRhs();
    return (lhs instanceof MinTimePointComparable || lhs instanceof MaxTimePointComparable ||
      rhs instanceof MinTimePointComparable || rhs instanceof MaxTimePointComparable);
  }

  /**
   * Returns a list of all variables that are relevant for computing new bounds.
   * Every variable that occurs in a relevant comparison is relevant.
   *
   * @return list of all relevant variables
   */
  private Set<String> getRelevantVariables() {
    Optional<Set<String>> vars =
      relevantComparisons.stream()
        .map(ComparisonExpressionTPGM::getVariables)
        .reduce((vars1, vars2) -> {
          vars1.addAll(vars2);
          return vars1;
        });
    return vars.orElseGet(HashSet::new);
  }

  @Override
  public TemporalCNF transformCNF(TemporalCNF cnf) throws QueryContradictoryException {
    query = cnf;
    relevantComparisons = getRelevantComparisons();
    relevantVariables = getRelevantVariables();
    initBounds();
    boolean changed = updateBounds();
    while (changed) {
      changed = updateBounds();
    }
    return augmentCNF();
  }

  /**
   * Appends constraints describing the inferred bounds to the query
   *
   * @return original cnf with comparisons corresponding to the inferred bounds
   * appended.
   */
  private TemporalCNF augmentCNF() {
    List<CNFElementTPGM> inferedConstraints = new ArrayList();
    for (String variable : relevantVariables) {
      HashMap<TimeSelector.TimeField, Long[]> newBounds =
        bounds.get(variable);
      HashMap<TimeSelector.TimeField, Long[]> oldBounds =
        initialBounds.get(variable);
      for (Map.Entry<TimeSelector.TimeField, Long[]> entryNew : newBounds.entrySet()) {
        long newLower = entryNew.getValue()[0];
        long newUpper = entryNew.getValue()[1];
        long oldLower = oldBounds.get(entryNew.getKey())[0];
        long oldUpper = oldBounds.get(entryNew.getKey())[1];
        if (newLower == newUpper && (oldLower != newLower || oldUpper != newUpper)) {
          inferedConstraints.add(singletonConstraint(
            new TimeSelector(variable, entryNew.getKey()), EQ,
            new TimeLiteral(newLower)));
        } else {
          if (newLower > Long.MIN_VALUE && newLower != oldLower) {
            // if the lower bound of tx_to is the same as the lower bound of tx_from
            // it is irrelevant (because this fact is not informative)
            if (entryNew.getKey() == TimeSelector.TimeField.TX_TO) {
              if (newLower == newBounds.get(TimeSelector.TimeField.TX_FROM)[0]) {
                continue;
              }
            }
            // same for valid
            if (entryNew.getKey() == TimeSelector.TimeField.VAL_TO) {
              if (newLower == newBounds.get(TimeSelector.TimeField.VAL_FROM)[0]) {
                continue;
              }
            }
            inferedConstraints.add(singletonConstraint(
              new TimeLiteral(newLower), LTE,
              new TimeSelector(variable, entryNew.getKey())));
          }

          if (newUpper < Long.MAX_VALUE && newUpper != oldUpper) {
            // if upper bound of tx_from is the same as upper bound for tx_to
            // it is irrelevant / uninformative
            if (entryNew.getKey() == TimeSelector.TimeField.TX_FROM) {
              if (newUpper == newBounds.get(TimeSelector.TimeField.TX_TO)[1]) {
                continue;
              }
            }
            // same for valid
            if (entryNew.getKey() == TimeSelector.TimeField.VAL_FROM) {
              if (newUpper == newBounds.get(TimeSelector.TimeField.VAL_TO)[1]) {
                continue;
              }
            }
            inferedConstraints.add(singletonConstraint(
              new TimeSelector(variable, entryNew.getKey()), LTE,
              new TimeLiteral(newUpper)));
          }
        }

      }
    }
    return query.and(new TemporalCNF(inferedConstraints));
  }

  /**
   * Builds a single CNF constraint from a comparison
   *
   * @param lhs        left hand side of the comparison
   * @param comparator comparator of the comparison
   * @param rhs        right hand side of the comparison
   * @return single CNF constraint containing the comparison
   */
  CNFElementTPGM singletonConstraint(ComparableExpression lhs, Comparator comparator,
                                     ComparableExpression rhs) {
    return new CNFElementTPGM(
      Collections.singletonList(new ComparisonExpressionTPGM(
        new Comparison(lhs, comparator, rhs)
      )));
  }

  /**
   * Updates all bounds for all variables
   *
   * @return true iff a value has been changed
   * @throws QueryContradictoryException if a contradiction in the query is detected
   */
  private boolean updateBounds() throws QueryContradictoryException {
    boolean changed = false;
    for (String variable : relevantVariables) {
      List<ComparisonExpressionTPGM> necessaryConditions = getNecessaryConditions(variable);
      for (ComparisonExpressionTPGM comp : necessaryConditions) {
        changed = updateBounds(variable, comp) || changed;
      }
    }
    return changed;
  }

  /**
   * Returns all necessary comparisons in the query that contain a certain variable
   *
   * @param variable variable to look for in the necessary comparisons
   * @return list of all necessary comparisons containing the variable
   */
  private List<ComparisonExpressionTPGM> getNecessaryConditions(String variable) {
    return relevantComparisons.stream()
      .filter(comparison ->
        comparison.getVariables().contains(variable))
      .collect(Collectors.toList());
  }

  /**
   * Updates the bounds for one variable using one comparison that involves this variable
   *
   * @param variable   the variable to update the bounds of
   * @param comparison comparison used to update the bounds
   * @return true iff bounds changed
   * @throws QueryContradictoryException if a contradiction in the query is detected
   */
  private boolean updateBounds(String variable, ComparisonExpressionTPGM comparison)
    throws QueryContradictoryException {
    ComparableExpression lhs = comparison.getLhs().getWrappedComparable();
    Comparator comparator = comparison.getComparator();
    ComparableExpression rhs = comparison.getRhs().getWrappedComparable();
    boolean changed = false;
    if (lhs instanceof TimeSelector) {
      changed = updateBounds((TimeSelector) lhs, comparator, rhs) || changed;
    } else if (rhs instanceof TimeSelector) {
      // comparisons of two time selectors are handled in the case above
      changed = updateBounds((TimeSelector) rhs, switchComparator(comparator), lhs) || changed;
    }
    return changed;
  }

  /**
   * Used to switch RHS and LHS of a comparison. This method switches the comparator.
   * E.g., < would be switched to >
   * This is not the inverse of a comparator
   *
   * @param comparator comparator to switch
   * @return switched comparator
   */
  private Comparator switchComparator(Comparator comparator) {
    if (comparator == EQ || comparator == Comparator.NEQ) {
      return comparator;
    } else if (comparator == LTE) {
      return Comparator.GTE;
    } else if (comparator == LT) {
      return Comparator.GT;
    } else if (comparator == Comparator.GT) {
      return LT;
    } else {
      return LTE;
    }
  }

  /**
   * Updates the bounds of a certain variable according to a comparison where
   * a time selector of this variable is the LHS
   *
   * @param selector   lhs (time selector)
   * @param comparator comparator of the comparison
   * @param rhs        rhs of the comparison
   * @return true iff the bounds changed
   * @throws QueryContradictoryException if a contradiction in the query is encountered
   */
  private boolean updateBounds(TimeSelector selector, Comparator comparator, ComparableExpression rhs)
    throws QueryContradictoryException {
    boolean changed = false;
    String lhsVariable = selector.getVariable();
    TimeSelector.TimeField lhsField = selector.getTimeProp();
    Long[] lhsBounds = bounds.get(lhsVariable).get(lhsField);
    Long lhsLower = lhsBounds[0];
    Long lhsUpper = lhsBounds[1];
    if (rhs instanceof TimeSelector) {
      String rhsVariable = rhs.getVariable();
      TimeSelector.TimeField rhsField = ((TimeSelector) rhs).getTimeProp();
      Long[] rhsBounds = bounds.get(rhsVariable).get(rhsField);
      Long rhsLower = rhsBounds[0];
      Long rhsUpper = rhsBounds[1];
      // only EQ, LT, LTE possible for comparisons between two selectors, as query is normalized
      if (comparator == EQ) {
        // errors
        if (lhsUpper < rhsLower || lhsLower > rhsUpper) {
          throw new QueryContradictoryException();
        }
        if (lhsLower < rhsLower) {
          lhsLower = rhsLower;
          changed = true;
        } else if (lhsLower > rhsLower) {
          rhsLower = lhsLower;
          changed = true;
        }
        if (lhsUpper > rhsUpper) {
          lhsUpper = rhsUpper;
          changed = true;
        } else if (lhsUpper < rhsUpper) {
          rhsUpper = lhsUpper;
          changed = true;
        }
      } else if (comparator == NEQ) {
        if (lhsLower.equals(lhsUpper) && rhsLower.equals(rhsUpper) && lhsLower.equals(rhsLower)) {
          throw new QueryContradictoryException();
        }
      } else if (comparator == LTE) {
        if (lhsLower > rhsUpper) {
          throw new QueryContradictoryException();
        }
        if (lhsUpper > rhsUpper) {
          lhsUpper = rhsUpper;
          changed = true;
        }
        if (lhsLower > rhsLower) {
          rhsLower = lhsLower;
          changed = true;
        }
      } else if (comparator == LT) {
        if (lhsLower >= rhsUpper) {
          throw new QueryContradictoryException();
        }
        if (lhsUpper > rhsUpper) {
          lhsUpper = rhsUpper - 1L;
          changed = true;
        }
        if (lhsLower > rhsLower) {
          rhsLower = lhsLower + 1L;
          changed = true;
        }
      } else {
        return false;
      }
      if (changed) {
        if (!(lhsLower <= lhsUpper) || !(rhsLower <= rhsUpper)) {
          throw new QueryContradictoryException();
        }
        bounds.get(lhsVariable).put(lhsField, new Long[] {lhsLower, lhsUpper});
        bounds.get(rhsVariable).put(rhsField, new Long[] {rhsLower, rhsUpper});
      }
    } else if (rhs instanceof TimeLiteral) {
      Long literal = ((TimeLiteral) rhs).getMilliseconds();
      if (comparator == Comparator.EQ) {
        // error
        if (lhsLower.equals(lhsUpper) && !lhsLower.equals(literal)) {
          throw new QueryContradictoryException();
        } else if (lhsLower > literal || lhsUpper < literal) {
          throw new QueryContradictoryException();
        } else if (!lhsLower.equals(lhsUpper)) {
          lhsLower = literal;
          lhsUpper = literal;
          changed = true;
        }
      } else if (comparator == NEQ) {
        if (lhsLower.equals(lhsUpper) && lhsLower.equals(literal)) {
          throw new QueryContradictoryException();
        }
      } else if (comparator == LT) {
        if (lhsLower >= literal) {
          throw new QueryContradictoryException();
        } else if (lhsUpper >= literal) {
          lhsUpper = literal - 1;
          changed = true;
        }
      } else if (comparator == LTE) {
        if (lhsLower > literal) {
          throw new QueryContradictoryException();
        } else if (lhsUpper > literal) {
          lhsUpper = literal;
          changed = true;
        }
      } else if (comparator == GT) {
        if (lhsUpper <= literal) {
          throw new QueryContradictoryException();
        } else if (lhsLower <= literal) {
          lhsLower = literal + 1;
          changed = true;
        }
      } else if (comparator == GTE) {
        if (lhsUpper < literal) {
          throw new QueryContradictoryException();
        } else if (lhsLower < literal) {
          lhsLower = literal;
          changed = true;
        }
      }
      if (changed) {
        if (!(lhsLower <= lhsUpper)) {
          throw new QueryContradictoryException();
        }
        bounds.get(lhsVariable).put(lhsField, new Long[] {lhsLower, lhsUpper});
      }
    }
    return changed;
  }

  public TemporalCNF getQuery() {
    return query;
  }

  public void setQuery(TemporalCNF query) {
    this.query = query;
  }

  public void setRelevantComparisons(
    List<ComparisonExpressionTPGM> relevantComparisons) {
    this.relevantComparisons = relevantComparisons;
  }

  public void setRelevantVariables(Set<String> relevantVariables) {
    this.relevantVariables = relevantVariables;
  }

  public HashMap<String, HashMap<TimeSelector.TimeField, Long[]>> getBounds() {
    return bounds;
  }

  public void setBounds(
    HashMap<String, HashMap<TimeSelector.TimeField, Long[]>> bounds) {
    this.bounds = bounds;
  }

  public HashMap<String, HashMap<TimeSelector.TimeField, Long[]>> getInitialBounds() {
    return initialBounds;
  }

  public void setInitialBounds(
    HashMap<String, HashMap<TimeSelector.TimeField, Long[]>> initialBounds) {
    this.initialBounds = initialBounds;
  }
}
