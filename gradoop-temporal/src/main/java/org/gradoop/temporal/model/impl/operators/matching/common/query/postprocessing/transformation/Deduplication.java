package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.QueryTransformation;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.CNFElementTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Removes duplicate clauses from the CNF
 */
public class Deduplication implements QueryTransformation {

    @Override
    public TemporalCNF transformCNF(TemporalCNF cnf) throws QueryContradictoryException {
        ArrayList<CNFElementTPGM> newClauses = new ArrayList<>();
        for(CNFElementTPGM clause: cnf.getPredicates()){
            if(!newClauses.contains(clause)){
                newClauses.add(clause);
            }
        }
        return new TemporalCNF(newClauses);
    }
}
