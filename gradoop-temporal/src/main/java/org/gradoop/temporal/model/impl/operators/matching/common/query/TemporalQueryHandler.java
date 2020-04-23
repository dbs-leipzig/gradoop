package org.gradoop.temporal.model.impl.operators.matching.common.query;

import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.util.QueryPredicateFactory;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpansionCriteria;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.exceptions.BailSyntaxErrorStrategy;

/**
 * Wraps a {@link GDLHandler} and adds functionality needed for query
 * processing during graph pattern matching.
 * Extension for temporal queries
 */
public class TemporalQueryHandler extends QueryHandler {
    /**
     * GDL handler
     */
    private final GDLHandler gdlHandler;

    /**
     * Creates a new query handler.
     *
     * @param gdlString GDL query string
     */
    public TemporalQueryHandler(String gdlString) {
        super(gdlString);
        gdlHandler = new GDLHandler.Builder()
                .setDefaultGraphLabel(GradoopConstants.DEFAULT_GRAPH_LABEL)
                .setDefaultVertexLabel(GradoopConstants.DEFAULT_VERTEX_LABEL)
                .setDefaultEdgeLabel(GradoopConstants.DEFAULT_EDGE_LABEL)
                .setErrorStrategy(new BailSyntaxErrorStrategy())
                .buildFromString(gdlString);
    }

    @Override
    public CNF getPredicates() {
        if (gdlHandler.getPredicates().isPresent()) {
            return QueryPredicateFactory.createFrom(gdlHandler.getPredicates().get()).asCNF();
        } else {
            return new CNF();
        }
    }

    /**
     * Returns the expansion conditions for a path.
     * @param startVariable
     * @return
     */
    public ExpansionCriteria getExpansionCondition(String startVariable) {
        //TODO implement
        return new ExpansionCriteria();
    }
}
