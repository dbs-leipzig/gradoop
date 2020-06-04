package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.QueryTransformation;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.CNFElementTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.util.*;
import java.util.stream.Collectors;

import static org.s1ck.gdl.utils.Comparator.*;

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
    TemporalCNF query;

    /**
     * All comparisons that make up a single clause in the CNF
     */
    List<ComparisonExpressionTPGM> relevantComparisons;

    /**
     * All variables in relevant comparisons
     */
    Set<String> relevantVariables;

    /**
     * Assigns every query variable for every time field (tx_from, tx_to, val_from, val_to)
     * a lower bound and an upper bound
     */
    HashMap<String, HashMap<TimeSelector.TimeField, Long[]>> bounds;

    /**
     * stores the explicit bounds that are stated in the CNF
     */
    HashMap<String, HashMap<TimeSelector.TimeField, Long[]>> initialBounds;


    /**
     * Initializes the bounds for each variable.
     * The initial lower bound of every time field (tx_from, tx_to, val_from, val_to)
     * is {@code Long.MIN_VALUE}, the upper bound {@code Long.MAX_VALUE}
     */
    private void initBounds(){
        bounds = new HashMap<>();
        Long[] init = new Long[]{Long.MIN_VALUE, Long.MAX_VALUE};
        for(String variable: getRelevantVariables()){
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
     * @return list of all necessary temporal comparisons in the CNF
     */
    private List<ComparisonExpressionTPGM> getRelevantComparisons(){
        return query.getPredicates().stream()
                .filter(clause -> clause.size()==1
                        && clause.getPredicates().get(0).isTemporal())
                .map(clause -> clause.getPredicates().get(0))
                .collect(Collectors.toList());
    }

    /**
     * Returns a list of all variables that are relevant for computing new bounds.
     * Every variable that occurs in a relevant comparison is relevant.
     * @return list of all relevant variables
     */
    private Set<String> getRelevantVariables() {
        Optional<Set<String>> vars =
                relevantComparisons.stream()
                .map(ComparisonExpressionTPGM::getVariables)
                .reduce((vars1, vars2)
                        -> {vars1.addAll(vars2); return vars1;});
        return vars.orElseGet(HashSet::new);
    }

    @Override
    public TemporalCNF transformCNF(TemporalCNF cnf) throws QueryContradictoryException {
        query = cnf;
        relevantComparisons = getRelevantComparisons();
        relevantVariables = getRelevantVariables();
        initBounds();
        boolean changed = updateBounds();
        while(changed){
            changed = updateBounds();
        }
        return augmentCNF();
    }

    /**
     * Appends constraints describing the inferred bounds to the query
     * @return original cnf with comparisons corresponding to the inferred bounds
     * appended.
     */
    private TemporalCNF augmentCNF() {
        List<CNFElementTPGM> inferedConstraints = new ArrayList();
        for(String variable: relevantVariables){
            HashMap<TimeSelector.TimeField, Long[]> newBounds =
                    bounds.get(variable);
            HashMap<TimeSelector.TimeField, Long[]> oldBounds =
                    initialBounds.get(variable);
            for(TimeSelector.TimeField field: newBounds.keySet()){
                long newLower = newBounds.get(field)[0];
                long newUpper = newBounds.get(field)[1];
                long oldLower = oldBounds.get(field)[0];
                long oldUpper = oldBounds.get(field)[1];
                if(newLower==newUpper && (oldLower!=newLower || oldUpper!=newUpper)){
                    inferedConstraints.add(singletonConstraint(
                            new TimeSelector(variable, field), EQ,
                            new TimeLiteral(newLower)));
                }
                else{
                    if(newLower > Long.MIN_VALUE && newLower!=oldLower){
                        // if the lower bound of tx_to is the same as the lower bound of tx_from
                        // it is irrelevant (because this fact is not informative)
                        if(field== TimeSelector.TimeField.TX_TO){
                            if(newLower == newBounds.get(TimeSelector.TimeField.TX_FROM)[0]){
                                continue;
                            }
                        }
                        // same for valid
                        if(field== TimeSelector.TimeField.VAL_TO){
                            if(newLower == newBounds.get(TimeSelector.TimeField.VAL_FROM)[0]){
                                continue;
                            }
                        }
                        inferedConstraints.add(singletonConstraint(
                                new TimeLiteral(newLower), LTE,
                                new TimeSelector(variable, field)));
                    }

                    if(newUpper < Long.MAX_VALUE && newUpper!=oldUpper){
                        // if upper bound of tx_from is the same as upper bound for tx_to
                        // it is irrelevant / uninformative
                        if(field== TimeSelector.TimeField.TX_FROM){
                            if(newUpper == newBounds.get(TimeSelector.TimeField.TX_TO)[1]){
                                continue;
                            }
                        }
                        // same for valid
                        if(field== TimeSelector.TimeField.VAL_FROM){
                            if(newUpper == newBounds.get(TimeSelector.TimeField.VAL_TO)[1]){
                                continue;
                            }
                        }
                        inferedConstraints.add(singletonConstraint(
                                new TimeSelector(variable, field), LTE,
                                new TimeLiteral(newUpper)));
                    }
                }

            }
        }
        return query.and(new TemporalCNF(inferedConstraints));
    }

    /**
     * Builds a single CNF constraint from a comparison
     * @param lhs left hand side of the comparison
     * @param comparator comparator of the comparison
     * @param rhs right hand side of the comparison
     * @return single CNF constraint containing the comparison
     */
    CNFElementTPGM singletonConstraint(ComparableExpression lhs, Comparator comparator, ComparableExpression rhs){
        return new CNFElementTPGM(
                Collections.singletonList(new ComparisonExpressionTPGM(
                        new Comparison(lhs, comparator, rhs)
                )));
    }

    /**
     * Updates all bounds for all variables
     * @return true iff a value has been changed
     */
    private boolean updateBounds() throws QueryContradictoryException {
        boolean changed = false;
        for(String variable: relevantVariables){
            List<ComparisonExpressionTPGM> necessaryConditions = getNecessaryConditions(variable);
            for(ComparisonExpressionTPGM comp: necessaryConditions){
                changed = updateBounds(variable, comp) || changed;
            }
        }
        return changed;
    }

    /**
     * Returns all necessary comparisons in the query that contain a certain variable
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
     * @param variable the variable to update the bounds of
     * @param comparison comparison used to update the bounds
     * @return true iff bounds changed
     */
    private boolean updateBounds(String variable, ComparisonExpressionTPGM comparison) throws QueryContradictoryException {
        ComparableExpression lhs = comparison.getLhs().getWrappedComparable();
        Comparator comparator = comparison.getComparator();
        ComparableExpression rhs = comparison.getRhs().getWrappedComparable();
        boolean changed = false;
        if(lhs instanceof TimeSelector){
            changed = updateBounds((TimeSelector) lhs, comparator, rhs) || changed;
        }
        // comparisons of two time selectors are handled in the case above
        else if(rhs instanceof TimeSelector){
            changed = updateBounds((TimeSelector) rhs, switchComparator(comparator), lhs) || changed;
        }
        return changed;
    }

    /**
     * Used to switch RHS and LHS of a comparison. This method switches the comparator.
     * E.g., < would be switched to >
     * This is not the inverse of a comparator
     * @param comparator comparator to switch
     * @return switched comparator
     */
    private Comparator switchComparator(Comparator comparator) {
        if(comparator == EQ || comparator == Comparator.NEQ){
            return comparator;
        } else if(comparator == LTE){
            return Comparator.GTE;
        } else if(comparator == LT){
            return Comparator.GT;
        } else if(comparator == Comparator.GT){
            return LT;
        } else{
            return LTE;
        }
    }

    /**
     * Updates the bounds of a certain variable according to a comparison where
     * a time selector of this variable is the LHS
     * @param selector lhs (time selector)
     * @param comparator comparator of the comparison
     * @param rhs rhs of the comparison
     * @return true iff the bounds changed
     */
    private boolean updateBounds(TimeSelector selector, Comparator comparator, ComparableExpression rhs) throws QueryContradictoryException {
        boolean changed = false;
        String lhsVariable = selector.getVariable();
        TimeSelector.TimeField lhsField = selector.getTimeProp();
        Long[] lhsBounds = bounds.get(lhsVariable).get(lhsField);
        Long lhsLower = lhsBounds[0];
        Long lhsUpper = lhsBounds[1];
        if(rhs instanceof TimeSelector){
            String rhsVariable = rhs.getVariable();
            TimeSelector.TimeField rhsField = ((TimeSelector) rhs).getTimeProp();
            Long[] rhsBounds = bounds.get(rhsVariable).get(rhsField);
            Long rhsLower = rhsBounds[0];
            Long rhsUpper = rhsBounds[1];
            // only EQ, LT, LTE possible for comparisons between two selectors, as query is normalized
            if(comparator == EQ){
                // errors
                if(lhsUpper < rhsLower || lhsLower > rhsUpper){
                    throw new QueryContradictoryException();
                }
                if(lhsLower < rhsLower){
                    lhsLower = rhsLower;
                    changed = true;
                } else if(lhsLower > rhsLower){
                    rhsLower = lhsLower;
                    changed = true;
                }
                if(lhsUpper > rhsUpper){
                    lhsUpper = rhsUpper;
                    changed = true;
                } else if (lhsUpper < rhsUpper){
                    rhsUpper = lhsUpper;
                    changed = true;
                }
            }else if(comparator == NEQ){
                if(lhsLower.equals(lhsUpper) && rhsLower.equals(rhsUpper) && lhsLower==rhsLower){
                    throw new QueryContradictoryException();
                }
            } else if(comparator == LTE){
                if(lhsLower > rhsUpper){
                    throw new QueryContradictoryException();
                }
                if(lhsUpper > rhsUpper){
                    lhsUpper = rhsUpper;
                    changed = true;
                }
                if (lhsLower > rhsLower){
                    rhsLower = lhsLower;
                    changed = true;
                }
            } else if(comparator == LT){
                if(lhsLower >= rhsUpper){
                    throw new QueryContradictoryException();
                }
                if(lhsUpper > rhsUpper){
                    lhsUpper = rhsUpper - 1L;
                    changed = true;
                }
                if (lhsLower > rhsLower){
                    rhsLower = lhsLower+1L;
                    changed = true;
                }
            } else{
                return false;
            }
            if(changed) {
                bounds.get(lhsVariable).put(lhsField, new Long[]{lhsLower, lhsUpper});
                bounds.get(rhsVariable).put(rhsField, new Long[]{rhsLower, rhsUpper});
            }
        }
        else if(rhs instanceof TimeLiteral){
            Long literal = ((TimeLiteral) rhs).getMilliseconds();
            if(comparator==Comparator.EQ){
                // error
                if(lhsLower.equals(lhsUpper) && !lhsLower.equals(literal)){
                    throw new QueryContradictoryException();
                } else if(lhsLower > literal || lhsUpper < literal){
                    throw new QueryContradictoryException();
                } else if (!lhsLower.equals(lhsUpper)){
                    lhsLower = literal;
                    lhsUpper = literal;
                    changed = true;
                }
            } else if(comparator == NEQ){
                if(lhsLower.equals(lhsUpper) && lhsLower.equals(literal)){
                    throw new QueryContradictoryException();
                }
            }else if(comparator == LT){
                if(lhsLower >= literal){
                    throw new QueryContradictoryException();
                } else if(lhsUpper >= literal){
                    lhsUpper = literal -1;
                    changed = true;
                }
            } else if(comparator == LTE){
                if(lhsLower > literal){
                    throw new QueryContradictoryException();
                } else if(lhsUpper > literal){
                    lhsUpper = literal;
                    changed = true;
                }
            } else if(comparator == GT){
                if(lhsUpper <= literal){
                    throw new QueryContradictoryException();
                } else if(lhsLower <= literal){
                    lhsLower = literal +1;
                    changed = true;
                }
            } else if(comparator == GTE){
                if(lhsUpper < literal){
                    throw new QueryContradictoryException();
                } else if(lhsLower < literal){
                    lhsLower = literal;
                    changed = true;
                }
            }
            if(changed){
                bounds.get(lhsVariable).put(lhsField, new Long[]{lhsLower, lhsUpper});
            }
        }
        return changed;
    }
}
