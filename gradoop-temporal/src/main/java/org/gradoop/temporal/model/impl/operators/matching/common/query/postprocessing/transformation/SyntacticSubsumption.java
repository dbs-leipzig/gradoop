package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;

/**
 * Simplifies a CNF by subsuming clauses syntactically.
 * The transformation does not consider semantic information, but only syntax
 * !!! This class assumes input CNFs to be normalized, i.e. not to contain < or <= !!!
 */
public class SyntacticSubsumption extends Subsumption {

    @Override
    public boolean subsumes(ComparisonExpressionTPGM c1, ComparisonExpressionTPGM c2){
        return c1.equals(c2);
    }
}
