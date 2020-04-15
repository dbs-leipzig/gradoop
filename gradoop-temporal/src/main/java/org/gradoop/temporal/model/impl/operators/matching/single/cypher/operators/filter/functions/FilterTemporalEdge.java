package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

/**
 * Filters an Edge by a given predicate.
 * Extended to support temporal edges
 */
public class FilterTemporalEdge implements FilterFunction<TemporalEdge> {
    /**
     * Filter predicate
     */
    private final CNF predicates;

    /**
     * Creates a new UDF
     *
     * @param predicates filter predicates
     */
    public FilterTemporalEdge(CNF predicates) {
        this.predicates = predicates;
    }

    @Override
    public boolean filter(TemporalEdge edge) throws Exception {
        return predicates.evaluate(edge);
    }
}
