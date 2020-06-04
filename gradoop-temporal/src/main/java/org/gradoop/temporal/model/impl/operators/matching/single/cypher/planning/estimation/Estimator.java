package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation;

import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;

/**
 * Base class for estimators that provides some utility methods.
 */
public abstract class Estimator {
    /**
     * Query handler to get information about query elements
     */
    private final TemporalQueryHandler queryHandler;
    /**
     * Statistics about the search graph
     */
    private final TemporalGraphStatistics graphStatistics;

    /**
     * Creates a new estimator.
     *
     * @param queryHandler query handler
     * @param graphStatistics graph statistics
     */
    Estimator(TemporalQueryHandler queryHandler, TemporalGraphStatistics graphStatistics) {
        this.queryHandler = queryHandler;
        this.graphStatistics = graphStatistics;
    }

    public TemporalQueryHandler getQueryHandler() {
        return queryHandler;
    }

    public TemporalGraphStatistics getGraphStatistics() {
        return graphStatistics;
    }

    /**
     * Returns the label of the given variable.
     *
     * @param variable query variable
     * @param isVertex true, iff the variable maps to a vertex
     * @return label
     */
    String getLabel(String variable, boolean isVertex) {
        return isVertex ? queryHandler.getVertexByVariable(variable).getLabel() :
                queryHandler.getEdgeByVariable(variable).getLabel();
    }

    /**
     * Returns the cardinality of the specified label according to the provided statistics.
     *
     * @param label label
     * @param isVertex true, iff label maps to a vertex
     * @return number of elements with the given label
     */
    long getCardinality(String label, boolean isVertex) {
        return isVertex ?
                graphStatistics.getVertexCount(label) :
                graphStatistics.getEdgeCount(label);
    }
}
