package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeLiteralComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeSelectorComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.utils.Comparator;

import static org.s1ck.gdl.utils.Comparator.*;

/**
 * Uses temporal information to subsume constraints that compare a time selector to a time literal.
 * Here, a temporal comparison c1 subsumes a temporal comparison c2 iff
 * c1 logically implies c2 and c1 is not equal to c2.
 * !!! This class assumes the input to be normalized, i.e. not to contain < or <= !!!
 */
public class TemporalSubsumption extends Subsumption {
    @Override
    public boolean subsumes(ComparisonExpressionTPGM c1, ComparisonExpressionTPGM c2) {
        if(!structureMatches(c1,c2)){
            return false;
        }
        boolean selectorIsLeft = c1.getLhs() instanceof TimeSelectorComparable;
        if(!selectorIsLeft){
            c1 = new ComparisonExpressionTPGM(c1.getGDLComparison().switchSides());
            c2 = new ComparisonExpressionTPGM(c2.getGDLComparison().switchSides());
        }
        String var1 = ((TimeSelector)c1.getLhs().getWrappedComparable()).getVariable();
        String var2 = ((TimeSelector)c2.getLhs().getWrappedComparable()).getVariable();
        if(!var1.equals(var2)){
            return false;
        }
        TimeSelector.TimeField field1 = ((TimeSelector)c1.getLhs().getWrappedComparable()).getTimeProp();
        TimeSelector.TimeField field2 = ((TimeSelector)c2.getLhs().getWrappedComparable()).getTimeProp();
        if(!field1.equals(field2)){
            return false;
        }
        Comparator comparator1 = c1.getComparator();
        Comparator comparator2 = c2.getComparator();
        Long literal1 = ((TimeLiteral)c1.getRhs().getWrappedComparable()).getMilliseconds();
        Long literal2 = ((TimeLiteral)c2.getRhs().getWrappedComparable()).getMilliseconds();
        return implies(comparator1, literal1, comparator2, literal2) && !c1.equals(c2);
    }

    /**
     * Checks if a comparison constraint (a comparator1 literal1) implies a comparison
     * constraint (a comparator2 literal2).
     * @param comparator1 comparator of the potentially implying constraint
     * @param literal1 rhs literal of the potentially implying constraint
     * @param comparator2 comparator of the potentially implied constraint
     * @param literal2 rhs literal of the potentially implied constraint
     * @return true iff (a comparator1 literal1) implies (a comparator2 literal2)
     */
    private boolean implies(Comparator comparator1, Long literal1, Comparator comparator2, Long literal2) {
        if(comparator1==Comparator.EQ){
            if(comparator2==Comparator.EQ){
                return literal1.equals(literal2);
            } else if(comparator2== NEQ){
                return !literal1.equals(literal2);
            } else if(comparator2== LTE){
                return literal1 <= literal2;
            } else if(comparator2== LT){
                return literal1 < literal2;
            }
        } else if(comparator1==NEQ){
            return false;
        } else if(comparator1==LTE){
            if(comparator2==Comparator.EQ){
                return false;
            } else if(comparator2==Comparator.NEQ){
                return literal1 <= literal2;
            } else if(comparator2== LTE){
                return literal1 <= literal2;
            } else if(comparator2==LT){
                return literal1 < literal2;
            }
        } else if(comparator1==LT){
            if(comparator2==EQ){
                return false;
            } else if(comparator2==NEQ){
                return literal1 < literal2;
            } else if(comparator2==LTE){
                return literal1 <= literal2;
            } else if(comparator2==LT){
                return literal1 < literal2;
            }
        }
        return false;
    }

    /**
     * Checks if two comparisons "match" for a subsumtion.
     * Here, only comparisons comparing a selector to a literal are relevant.
     * As the CNF is assumed to be normalized (no <,<=), comparisons c1 and
     * c2 are relevant iff they both have the form (selector comparator literal)
     * or both have the form (literal comparator selector)
     * @param c1 comparison
     * @param c2 comparison
     * @return true iff the structures of c1 and c2 match
     * according to the criteria defined above
     */
    private boolean structureMatches(ComparisonExpressionTPGM c1, ComparisonExpressionTPGM c2) {
        return ((c1.getLhs() instanceof TimeSelectorComparable
                && c2.getLhs() instanceof TimeSelectorComparable
                && c1.getRhs() instanceof TimeLiteralComparable
                && c2.getRhs() instanceof TimeLiteralComparable)
                ||
                (c1.getLhs() instanceof TimeLiteralComparable
                        && c2.getLhs() instanceof TimeLiteralComparable
                        && c1.getRhs() instanceof TimeSelectorComparable
                        && c2.getRhs() instanceof TimeSelectorComparable));
    }
}
