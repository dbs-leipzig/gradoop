package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.QueryTransformation;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.relationgraph.RelationGraph;
import org.s1ck.gdl.utils.Comparator;

import java.util.*;
import java.util.stream.Collectors;

import static org.s1ck.gdl.utils.Comparator.*;

/**
 * Builds a relation graph (cf. {@link RelationGraph}
 * from the necessary temporal comparisons in a CNF and checks
 * if it contains forbidden cyclic paths.
 * For example, a graph a<b=a would be forbidden, as it implies a contradiction
 * in the CNF.
 */
public class CheckForCircles implements QueryTransformation {

    @Override
    public TemporalCNF transformCNF(TemporalCNF cnf) throws QueryContradictoryException {
        List<List<Comparator>> cyclicPaths =
                new RelationGraph(
                        getNecessaryTemporalComparisons(cnf)
                ).findAllCircles();
        for(List<Comparator> cyclicPath: cyclicPaths){
            if(illegalCircle(cyclicPath)){
                throw new QueryContradictoryException();
            }
        }
        return cnf;
    }

    /**
     * Checks whether a cyclic path in the relations graph implies a contradictory query
     * @param cyclicPath cyclic path to check
     * @return true iff the cyclic path implies a contradictory query
     */
    private boolean illegalCircle(List<Comparator> cyclicPath) {
        int countEQ = Collections.frequency(cyclicPath, EQ);
        int countNEQ = Collections.frequency(cyclicPath, NEQ);
        int countLTE = Collections.frequency(cyclicPath, LTE);
        int countLT = Collections.frequency(cyclicPath, LT);

        return ((countNEQ==0 && countLT>=1) ||
                (countEQ>=1 && countNEQ==1 && countLTE==0 && countLT==0));
    }

    /**
     * Returns the set of necessary comparisons in the CNF, i.e. all comparisons that
     * form a singleton clause
     * @param cnf CNF
     * @return set of necessary comparisons
     */
    private Set<ComparisonExpressionTPGM> getNecessaryTemporalComparisons(TemporalCNF cnf){
        return cnf.getPredicates().stream()
                .filter(clause -> clause.size() == 1)
                .map(clause -> clause.getPredicates().get(0))
                .filter(comparison -> comparison.isTemporal())
                .collect(Collectors.toSet());
    }
}
