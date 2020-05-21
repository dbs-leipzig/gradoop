package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;

/**
 * Filters temporal vertices by a given predicate
 * practically identical to
 * {@link org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions.FilterVertex}
 * but adjusted to process TPGM vertices
 */
public class FilterTemporalVertex implements FilterFunction<TemporalVertex> {

    /**
     * Filter predicate
     */
    private final TemporalCNF predicates;

    /**
     * Creates a new UDF
     *
     * @param predicates filter predicates
     */
    public FilterTemporalVertex(TemporalCNF predicates) {
        this.predicates = predicates;
    }

    @Override
    public boolean filter(TemporalVertex vertex) throws Exception {
        return predicates.evaluate(vertex);
    }
}
