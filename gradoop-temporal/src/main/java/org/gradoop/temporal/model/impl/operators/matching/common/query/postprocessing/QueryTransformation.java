package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing;

import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;

/**
 * Common interface for all query transformations
 */
public interface QueryTransformation {

    /**
     * Applies a transformation on a CNF
     * @param cnf the CNF to transform
     * @return transformed CNF
     */
    public TemporalCNF transformCNF(TemporalCNF cnf) throws QueryContradictoryException;
}
