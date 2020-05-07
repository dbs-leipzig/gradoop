package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.util;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.ElementSelectorComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.LiteralComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.PropertySelectorComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.*;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.ElementSelector;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.PropertySelector;
import org.s1ck.gdl.model.comparables.time.*;

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
        } else if(expression.getClass() == MinTimePoint.class){
            return new MinTimePointComparable((MinTimePoint) expression);
        } else if(expression.getClass() == MaxTimePoint.class){
            return new MaxTimePointComparable((MaxTimePoint) expression);
        } else if(expression.getClass()==TimeConstant.class){
            return new TimeConstantComparable((TimeConstant) expression);
        } else if(expression.getClass()==Duration.class){
            return new DurationComparable((Duration)expression);
        }
        /*else if(expression.getClass() == PlusTimePoint.class){
            return new PlusTimePointComparable((PlusTimePoint) expression);
        }*/ else {
            throw new IllegalArgumentException(
                    expression.getClass() + " is not a GDL ComparableExpression"
            );
        }
    }
}
