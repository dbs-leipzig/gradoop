package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.QueryTransformation;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.CNFElementTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.utils.Comparator;

import java.util.stream.Collectors;

import static org.s1ck.gdl.utils.Comparator.LTE;

/**
 * Looks for trivial tautologies like {@code a.tx_from <= a.tx_to} or (b.prop=b.prop).
 * Every disjunctive clause containing a tautology is removed from the CNF.
 * !!! This class assumes input CNFs to be normalized, i.e. not to contain > or >= !!!
 */
public class TrivialTautologies implements QueryTransformation {
    @Override
    public TemporalCNF transformCNF(TemporalCNF cnf) {
        if(cnf.getPredicates().size()==0){
            return cnf;
        }
        return new TemporalCNF(
                cnf.getPredicates().stream()
                        .filter(this::notTautological)
                        .collect(Collectors.toList())
        );
    }

    /**
     * Checks whether a clause is not tautological, i.e. contains no tautological comparison
     * @param clause clause to check for tautologies
     * @return true iff the clause is not tautological
     */
    private boolean notTautological(CNFElementTPGM clause){
        for(ComparisonExpressionTPGM comparison: clause.getPredicates()){
            if(isTautological(comparison)){
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if a comparison is tautological. This check is "uninformed", it only employs
     * simple (domain) logic. The following comparisons are considered tautological:
     *  - x=x
     *  - x<=x
     *  - a.tx_from <= a.tx_to
     *  - a.val_from <= a.val_to
     *  - literal1 comp literal2    iff the comparison holds
     * @param comp comparison to check for tautology
     * @return true iff the comparison is tautological according to the criteria listed above.
     */
    private boolean isTautological(ComparisonExpressionTPGM comp){
        ComparableExpression lhs = comp.getLhs().getWrappedComparable();
        Comparator comparator = comp.getComparator();
        ComparableExpression rhs = comp.getRhs().getWrappedComparable();

        // x=x, x<=x
        if(lhs.equals(rhs)){
            if(comparator.equals(Comparator.EQ) || comparator.equals(LTE)){
                return true;
            }
        }
        // a.tx_from <= a.tx_to, a.val_from <= a.val_to
        if(lhs instanceof TimeSelector && rhs instanceof TimeSelector &&
            lhs.getVariable().equals(rhs.getVariable())){
            if(((TimeSelector) lhs).getTimeProp().equals(TimeSelector.TimeField.TX_FROM) &&
            ((TimeSelector) rhs).getTimeProp().equals(TimeSelector.TimeField.TX_TO) ||
                    ((TimeSelector) lhs).getTimeProp().equals(TimeSelector.TimeField.VAL_FROM) &&
                            ((TimeSelector) rhs).getTimeProp().equals(TimeSelector.TimeField.VAL_TO)){
                if(comparator.equals(LTE)){
                    return true;
                }
            }
        }
        // comparison of two (time) literals is tautological iff the comparison holds
        else if((lhs instanceof TimeLiteral && rhs instanceof TimeLiteral) ||
                (lhs instanceof Literal && rhs instanceof Literal)){
            // true iff the comparison holds
            return comp.evaluate(new TemporalVertex());
        }
        return false;
    }
}
