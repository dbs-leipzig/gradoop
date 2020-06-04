package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation.util;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.QueryComparableTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.LiteralComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.PropertySelectorComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.CNFElementTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.*;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.s1ck.gdl.model.comparables.time.*;
import org.s1ck.gdl.utils.Comparator;


import java.util.*;
import java.util.stream.Collectors;

import static org.s1ck.gdl.utils.Comparator.*;

public class CNFEstimation {

    /**
     * Graph statistics on which the estimations are based
     */
    private TemporalGraphStatistics stats;

    /**
     * maps variable names to their type (vertex or edge)
     */
    private Map<String, TemporalGraphStatistics.ElementType> typeMap;

    /**
     * maps variable names to labels (if known)
     */
    private Map<String, String> labelMap;

    /**
     * Creates a new object
     * @param stats graph statistics on which estimations are based
     * @param typeMap map from variable name to type (vertex/edge)
     */
    public CNFEstimation(TemporalGraphStatistics stats,
                         Map<String, TemporalGraphStatistics.ElementType> typeMap,
                         Map<String, String> labelMap){
        this.stats = stats;
        this.typeMap = typeMap;
        this.labelMap = labelMap;
    }

    /**
     * Estimates the probability that a CNF is true, based on the graph statistics.
     * Naive assumption: all contained comparisons are pair-wise independent.
     *
     * @param cnf cnf
     * @return estimation of the probability that the CNF evaluates to true
     */
    public double estimateCNF(TemporalCNF cnf){
        if(cnf.getPredicates().isEmpty()){
            return 1.;
        }
        return cnf.getPredicates().stream()
                .map(this::estimateCNFElement)
                .collect(Collectors.toList())
                .stream()
                .reduce((i,j) -> i*j).get();
    }

    /**
     * Estimates the probability that a {@link CNFElement}, i.e. a disjunction of
     * comparisons, is true (based on the graph statistics).
     *
     * @param element CNFElement
     * @return estimation of probability that the CNFElement evaluates to true
     */
    private double estimateCNFElement(CNFElementTPGM element){
        if(element.getPredicates().size()==1){
            return estimateComparison(element.getPredicates().get(0));
        }
        else{
            List<ComparisonExpressionTPGM> comparisons = element.getPredicates();
            double sum = 0.;
            double firstEst = estimateComparison(element.getPredicates().get(0));
            CNFElementTPGM rest = new CNFElementTPGM(element.getPredicates().subList(
                    1, element.getPredicates().size()));
            double restEst = estimateCNF(new TemporalCNF(Arrays.asList(rest)));
            return firstEst + restEst - (firstEst*restEst);
        }
    }


    /**
     * Creates a CNF from a list of comparisons (conjunction of the comparisons)
     * @param comparisons list of comparisons
     * @return CNF (conjunction) from a list of comparisons
     */
    private TemporalCNF cnfFrom(List<ComparisonExpressionTPGM> comparisons) {
        ArrayList<CNFElementTPGM> elements = new ArrayList<>();
        for(ComparisonExpressionTPGM comparison: comparisons){
            elements.add(new CNFElementTPGM(
                    new ArrayList<ComparisonExpressionTPGM>(Arrays.asList(comparison))));
        }
        return new TemporalCNF(elements);
    }

    /**
     * Estimates the probability that a comparison evaluates to true, based on
     * the graph statistics.
     *
     * @param comparisonExpression comparison
     * @return estimation of the probability that the comparison evaluates to true
     */
    private double estimateComparison(ComparisonExpressionTPGM comparisonExpression) {
        if(isLabelComp(comparisonExpression)){
            return 1.;
        }
        if(comparisonExpression.getVariables().size() > 1){
            return estimateComparisonOnDifferent(comparisonExpression);
        }
        else{
            return estimateCompOnSameTPGM(comparisonExpression);
        }
    }

    /**
     * Checks if a comparison describes the label of an element
     * @param comparison comparison to check
     * @return true iff the comparison describes the label of an element
     */
    private boolean isLabelComp(ComparisonExpressionTPGM comparison){
        if(comparison.getComparator()!=EQ){
            return false;
        }
        if(comparison.getLhs() instanceof PropertySelectorComparable){
            if(((PropertySelectorComparable) comparison.getLhs())
                    .getPropertyKey().equals("__label__")){
                return true;
            }
        } else if(comparison.getRhs() instanceof PropertySelectorComparable){
            if(((PropertySelectorComparable) comparison.getRhs())
                    .getPropertyKey().equals("__label__")){
                return true;
            }
        }
        return false;
    }

    /**
     * Estimates the probability that a comparison involving only one element holds.
     * @param comparisonExpression comparison
     * @return estimation of probability that the comparison holds
     */
    private double estimateCompOnSameTPGM(ComparisonExpressionTPGM comparisonExpression) {
        QueryComparableTPGM lhs = comparisonExpression.getLhs();
        QueryComparableTPGM rhs = comparisonExpression.getRhs();
        Comparator comp = comparisonExpression.getComparator();

        if(comparisonExpression.isTemporal()){
            if(lhs instanceof TimeSelectorComparable && rhs instanceof TimeSelectorComparable){
                // TODO implement?
                return 1.;
            }
            else if(lhs instanceof TimeSelectorComparable && rhs instanceof TimeLiteralComparable
                    || lhs instanceof TimeLiteralComparable && rhs instanceof TimeSelectorComparable){
                return simpleTemporalEstimation((ComparisonExpressionTPGM) comparisonExpression);
            }
            else if(lhs instanceof MinTimePointComparable || lhs instanceof MaxTimePointComparable
                    || rhs instanceof MinTimePointComparable || rhs instanceof MaxTimePointComparable){
                return MinMaxTemporalEstimation((ComparisonExpressionTPGM) comparisonExpression);
            }
            else if((lhs instanceof DurationComparable && rhs instanceof TimeConstantComparable)
                    || (lhs instanceof TimeConstantComparable && rhs instanceof DurationComparable)){
                return simpleDurationEstimation((ComparisonExpressionTPGM) comparisonExpression);
            } else{
                return 1.;
            }
        }
        else{
            if((lhs instanceof PropertySelectorComparable
                    && rhs instanceof LiteralComparable)
                    || (lhs instanceof LiteralComparable
                    && rhs instanceof PropertySelectorComparable) ){
                return simplePropertyEstimation(comparisonExpression);
            } else if(lhs instanceof PropertySelectorComparable &&
                    rhs instanceof PropertySelectorComparable){
                return complexPropertyEstimation(comparisonExpression);
            }
            else{
                return 1.;
            }
        }
    }

    /**
     * Estimates the probability that a comparison between a tx or val duration and
     * a time constant evaluates to true.
     * All other possible comparisons involving a duration are estimated 1.0
     * @param comparisonExpression comparison
     * @return estimation of the probability that the comparison evaluates to true
     */
    private double simpleDurationEstimation(ComparisonExpressionTPGM comparisonExpression) {
        QueryComparableTPGM lhs = comparisonExpression.getLhs();
        QueryComparableTPGM rhs = comparisonExpression.getRhs();
        Comparator comp = comparisonExpression.getComparator();
        // ensure that the duration is always on the left side
        if(rhs instanceof DurationComparable){
            QueryComparableTPGM t = lhs;
            lhs = rhs;
            rhs = t;
            comp = switchComparator(comp);
        }
        if(!(rhs instanceof TimeConstantComparable)){
            return 1.;
        }
        // only valid and tx durations can be estimated
        Duration duration = (Duration) ((DurationComparable)lhs).getWrappedComparable();
        if(!(duration.getFrom() instanceof TimeSelector) || !(duration.getTo() instanceof TimeSelector)){
            return 1.;
        }
        TimeSelector from = (TimeSelector) duration.getFrom();
        TimeSelector to = (TimeSelector) duration.getTo();
        if(!
                ((from.getTimeProp()== TimeSelector.TimeField.VAL_FROM
                && to.getTimeProp()== TimeSelector.TimeField.VAL_TO) ||
                (from.getTimeProp()== TimeSelector.TimeField.TX_FROM
                && to.getTimeProp()== TimeSelector.TimeField.TX_TO))){
            return 1.;
        }

        String variable = new ArrayList<>(comparisonExpression.getVariables()).get(0);
        TemporalGraphStatistics.ElementType type = typeMap.get(variable);
        Optional<String> label = labelMap.containsKey(variable) ?
                Optional.of(labelMap.get(variable)) : Optional.empty();
        boolean transaction = from.getTimeProp()== TimeSelector.TimeField.TX_FROM;
        long rhsValue = ((TimeConstant)((TimeConstantComparable) rhs)
                .getWrappedComparable()).getMillis();

        return stats.estimateDurationProb(type, label, comp, transaction, rhsValue);
    }

    /**
     * Estimates the probability that a comparison involving MIN or MAX holds.
     * @param comparisonExpression comparison
     * @return estimation of probability that the comparison holds
     */
    private double MinMaxTemporalEstimation(ComparisonExpressionTPGM comparisonExpression) {
        // TODO implement if needed
        return 1.0;
    }


    /**
     * Computes the estimation of the probability that a comparison between a time selector
     * and a time literal holds
     * @param comparisonExpression comparison
     * @return estimation of the probability that the comparison holds
     */
    private double simpleTemporalEstimation(ComparisonExpressionTPGM comparisonExpression) {
        TemporalComparable lhs = (TemporalComparable) comparisonExpression.getLhs();
        TemporalComparable rhs = (TemporalComparable) comparisonExpression.getRhs();
        Comparator comp = comparisonExpression.getComparator();
        // adjust the comparison so that the selector is always on the lhs
        if(rhs instanceof TimeSelectorComparable){
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
     * @param comparisonExpression comparison
     * @return estimation of the probability that the comparison holds
     */
    private double complexPropertyEstimation(ComparisonExpressionTPGM comparisonExpression) {
        QueryComparableTPGM lhs = comparisonExpression.getLhs();
        QueryComparableTPGM rhs = comparisonExpression.getRhs();
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
     * @param comparisonExpression comparison
     * @return estimation of the probability that the comparison holds
     */
    private double simplePropertyEstimation(ComparisonExpressionTPGM comparisonExpression) {
        QueryComparableTPGM lhs = comparisonExpression.getLhs();
        QueryComparableTPGM rhs = comparisonExpression.getRhs();
        Comparator comp = comparisonExpression.getComparator();
        // "normalize" the comparison so that the selector is on the left side
        if(rhs instanceof PropertySelectorComparable){
            QueryComparableTPGM t = lhs;
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
     * @param comp comparator to switch
     * @return switched comparator
     */
    private Comparator switchComparator(Comparator comp) {
        if(comp==EQ || comp==NEQ){
            return comp;
        } else if(comp==LT){
            return GT;
        } else if(comp==LTE){
            return GTE;
        } else if(comp==GT){
            return LT;
        } else {
            return LTE;
        }
    }

    /**
     * Estimates the probability that a comparison involving more than one element holds.
     * @param comparisonExpression comparison
     * @return estimation of probability that the comparison holds.
     */
    private double estimateComparisonOnDifferent(ComparisonExpressionTPGM comparisonExpression) {
        // TODO implement maybe later
        return 1.;
    }
}
