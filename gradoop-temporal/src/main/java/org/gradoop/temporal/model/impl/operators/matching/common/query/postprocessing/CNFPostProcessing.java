package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing;

import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation.*;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Postprocessing pipeline for CNFs.
 * Wraps a list of {@link QueryTransformation} objects, that are applied to a CNF
 */
public class CNFPostProcessing {

    /**
     * List of transformations. They are applied in the order of the list
     */
    List<QueryTransformation> transformations;

    /**
     * Creates a new custom postprocessing pipeline
     * @param transformations list of transformations to apply
     */
    public CNFPostProcessing(List<QueryTransformation> transformations){
        this.transformations = transformations;
    }

    /**
     * Creates a default postprocessing pipeline:
     * 1. Normalization
     * 2. "Translate" suitable Min/Max expressions to simple predicates
     * 3. subsumption on a purely syntactical basis
     * 4. checking for trivial tautologies, maybe simplifying the query
     * 5. checking for trivial contradictions, maybe simplifying the query
     * 6. checking for less trivial contradictions in temporal comparisons
     * 7. inferring implicit temporal information
     * 8. subsumption with temporal information
     * 9. deduplication of clauses
     */
    public CNFPostProcessing(){
        this(Arrays.asList(
                new Normalization(),
                new MinMaxUnfolding(),
                new SyntacticSubsumption(),
                new TrivialTautologies(),
                new TrivialContradictions(),
                new AddTrivialConstraints(),
                new CheckForCircles(),
                new InferBounds(),
                new TemporalSubsumption(),
                new TrivialTautologies()
                ));
    }

    /**
     * Executes the postprocessing pipeline on a CNF
     * @param cnf CNF to postprocess
     * @return postprocessed CNF
     * @throws QueryContradictoryException if the CNF is found to be contradictory
     * during the postprocessing
     */
    public TemporalCNF postprocess(TemporalCNF cnf) throws QueryContradictoryException {
        for(int i= 0; i<transformations.size(); i++){
            cnf = transformations.get(i).transformCNF(cnf);
        }
        return cnf;
    }


}
