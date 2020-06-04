package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.CNFElementTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for testing query transformations
 */
public class Util {

    /**
     * Builds a TemporalCNF from a set of lists of comparisons
     * @param clauses set of lists of comparisons
     * @return TemporalCNF from clauses
     */
    public static TemporalCNF cnfFromLists(List<Comparison>...clauses){
        List<CNFElementTPGM> cnfClauses = new ArrayList<>();
        for(List<Comparison> comparisons: clauses){
            ArrayList<ComparisonExpressionTPGM> wrappedComparisons = new ArrayList<>();
            for(Comparison comparison: comparisons){
                wrappedComparisons.add(new ComparisonExpressionTPGM(comparison));
            }
            cnfClauses.add(new CNFElementTPGM(wrappedComparisons));
        }
        return new TemporalCNF(cnfClauses);
    }
}
