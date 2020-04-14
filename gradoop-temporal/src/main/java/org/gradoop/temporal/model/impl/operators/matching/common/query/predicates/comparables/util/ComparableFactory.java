package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.util;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.ElementSelectorComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.LiteralComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.PropertySelectorComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.PlusTimePointComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeLiteralComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeSelectorComparable;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.ElementSelector;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.PropertySelector;
import org.s1ck.gdl.model.comparables.time.PlusTimePoint;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;

/**
 * Class for creating a {@link QueryComparable} wrapper for a GDL {@link ComparableExpression}
 */
public class ComparableFactory {

    /**
     * Create a wrapper for a GDL comparable
     *
     * @param expression the GDL element to wrap
     * @return wrapper for expression
     * @throws IllegalArgumentException if expression is no GDL ComparableExpression
     */
    public static QueryComparable createComparableFrom(ComparableExpression expression){
        if (expression.getClass() == Literal.class) {
            return new LiteralComparable((Literal) expression);
        } else if (expression.getClass() == PropertySelector.class) {
            return new PropertySelectorComparable((PropertySelector) expression);
        } else if (expression.getClass() == ElementSelector.class) {
            return new ElementSelectorComparable((ElementSelector) expression);
        } else if(expression.getClass() == TimeLiteral.class){
            return new TimeLiteralComparable((TimeLiteral) expression);
        } else if(expression.getClass() == TimeSelector.class){
            return new TimeSelectorComparable((TimeSelector) expression);
        } else if(expression.getClass() == PlusTimePoint.class){
            return new PlusTimePointComparable((PlusTimePoint) expression);
        } else {
            throw new IllegalArgumentException(
                    expression.getClass() + " is not a GDL ComparableExpression"
            );
        }
    }
}
