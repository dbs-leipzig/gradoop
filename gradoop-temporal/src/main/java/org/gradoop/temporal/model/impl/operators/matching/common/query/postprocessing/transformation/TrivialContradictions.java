package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.QueryTransformation;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.CNFElementTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.utils.Comparator;

import java.util.ArrayList;
import java.util.List;

import static org.s1ck.gdl.utils.Comparator.GT;
import static org.s1ck.gdl.utils.Comparator.LTE;

/**
 * Looks for trivial contradictions like {@code a.tx_from > a.tx_to} or (b.prop!=b.prop).
 * Every contradictory comparison is removed from its disjunctive clause.
 * If every comparison in a clause is contradictory, the whole CNF is a contradiction.
 * In this case, the transformation returns {@code null}
 * !!! This class assumes input CNFs to be normalized, i.e. not to contain < or <= !!!
 */
public class TrivialContradictions implements QueryTransformation {
    @Override
    public TemporalCNF transformCNF(TemporalCNF cnf) throws QueryContradictoryException {
        ArrayList<CNFElementTPGM> newClauses = new ArrayList<>();
        for(CNFElementTPGM clause: cnf.getPredicates()){
            CNFElementTPGM newClause = transformDisjunction(clause);
            newClauses.add(newClause);
        }
        return new TemporalCNF(newClauses);
    }

    private CNFElementTPGM transformDisjunction(CNFElementTPGM clause) throws QueryContradictoryException {
        List<ComparisonExpressionTPGM> oldComparisons = clause.getPredicates();
        ArrayList<ComparisonExpressionTPGM> newComparisons = new ArrayList<>();
        boolean contradiction = true;
        for(ComparisonExpressionTPGM comparison: oldComparisons){
            if(!isContradictory(comparison)){
                newComparisons.add(comparison);
                contradiction = false;
            }
        }
        if(contradiction){
            throw new QueryContradictoryException();
        }
        else{
            return new CNFElementTPGM(newComparisons);
        }
    }


    private boolean isContradictory(ComparisonExpressionTPGM comp) {
        ComparableExpression lhs = comp.getLhs().getWrappedComparable();
        Comparator comparator = comp.getComparator();
        ComparableExpression rhs = comp.getRhs().getWrappedComparable();

        // x<x, x!=x
        if(lhs.equals(rhs)){
            if(!(comparator.equals(Comparator.EQ) || comparator.equals(LTE))){
                return true;
            }
        }
        // a.tx_from > a.tx_to
        if(lhs instanceof TimeSelector && rhs instanceof TimeSelector &&
                lhs.getVariable().equals(rhs.getVariable())){
            if(((TimeSelector) lhs).getTimeProp().equals(TimeSelector.TimeField.TX_FROM) &&
                    ((TimeSelector) rhs).getTimeProp().equals(TimeSelector.TimeField.TX_TO) ||
                    ((TimeSelector) lhs).getTimeProp().equals(TimeSelector.TimeField.VAL_FROM) &&
                            ((TimeSelector) rhs).getTimeProp().equals(TimeSelector.TimeField.VAL_TO)){
                if(comparator.equals(GT)){
                    return true;
                }
            }
        }
        // comparison of two (time) literals is tautological iff the comparison holds
        else if((lhs instanceof TimeLiteral && rhs instanceof TimeLiteral) ||
                (lhs instanceof Literal && rhs instanceof Literal)){
            // true iff the comparison holds
            return !comp.evaluate(new TemporalVertex());
        }
        return false;
    }
}
