package org.gradoop.temporal.model.impl.operators.matching.common.statistics;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.temporal.model.impl.TemporalGraphFactory;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TemporalComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.utils.Comparator;

import javax.lang.model.element.TypeElement;
import java.util.Map;
import java.util.Optional;

public abstract class TemporalGraphStatistics {

    /**
     * Returns the factory to create instances.
     * @return factory
     */
    public abstract TemporalGraphStatisticsFactory getFactory();

    /**
     * Describes the two types of graph elements
     */
    public enum ElementType {VERTEX, EDGE};

    /**
     * Estimates the probability that a time value (tx_from, tx_to, valid_from, valid_to)
     * of an element with a certain label satisfies a constraint comparing it to a constant
     *
     * @param type1 the type of the element to compare (vertex or edge)
     * @param label1 label of the element to compare (if known)
     * @param field1 time field (tx_from, tx_to, valid_from, valid_to) of the element to compare
     * @param comp comparator
     * @param value long constant to compare the element with
     * @return estimation of the probability that the condition holds
     */
    public abstract double estimateTemporalProb(ElementType type1, Optional<String> label1,
                                                TimeSelector.TimeField field1,
                                                Comparator comp, Long value);

    /**
     * Estimates the probability that a comparison of the form
     * {@code variable.duration comparator constant}
     * holds.
     * @param type type of the lhs element (vertex/edge)
     * @param label label of the lhs element
     * @param comp comparator of the comparison
     * @param transaction indicates whether transaction time should be compared (=>true)
     *                    or valid time (=> false)
     * @param value lhs duration constant
     * @return estimated probability that the comparison holds
     */
    public abstract double estimateDurationProb(ElementType type, Optional<String> label,
                                                Comparator comp, boolean transaction, Long value);

    /**
     * Estimates the probability that a comparison of a property value with a constant holds
     * @param type type of the lhs (vertex/edge)
     * @param label label of the lhs, if known
     * @param comp comparator of the comparison
     * @param value rhs value
     * @return estimated probability that the comparison holds
     */
    public abstract double estimatePropertyProb(ElementType type, Optional<String> label, String property,
                                                Comparator comp, PropertyValue value);

    /**
     * Estimates the probability that a comparison between two property selectors holds
     * @param type1 type of the lhs element (vertex/edge)
     * @param label1 label of the lhs element, if known
     * @param property1 lhs property key
     * @param comp comparator of the comparison
     * @param type2 type of the rhs element (vertex/edge)
     * @param label2 label of the rhs element, if known
     * @param property2 rhs property key
     * @return estimated probability that the comparison holds
     */
    public abstract double estimatePropertyProb(ElementType type1, Optional<String> label1,
                                                String property1, Comparator comp,
                                                ElementType type2, Optional<String> label2,
                                                String property2);


}
