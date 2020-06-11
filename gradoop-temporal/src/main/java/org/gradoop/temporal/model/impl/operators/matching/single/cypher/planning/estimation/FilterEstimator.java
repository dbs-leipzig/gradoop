package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation;

import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.FilterNode;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectTemporalEdgesNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectTemporalVerticesNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.unary.FilterTemporalEmbeddingsNode;

import java.util.HashMap;

import static java.util.stream.Collectors.toMap;

/**
 * Keeps track of the leaf nodes in a query plan and computes a final selectivity factor resulting
 * from the applied predicates.
 */
public class FilterEstimator extends Estimator {
    /**
     * The non-filtered cardinality of the leaf node.
     */
    private long cardinality;
    /**
     * The resulting selectivity factor of all leaf predicates
     */
    private double selectivity;

    /**
     * Used to estimate filter selectivity
     */
    private CNFEstimation cnfEstimation;

    /**
     * Creates a new estimator.
     *
     * @param queryHandler query handler
     * @param graphStatistics graph statistics
     */
    FilterEstimator(TemporalQueryHandler queryHandler, TemporalGraphStatistics graphStatistics,
                    CNFEstimation cnfEstimation) {
        super(queryHandler, graphStatistics);
        this.selectivity = 1f;
        //initCNFEstimator();
        this.cnfEstimation = cnfEstimation;
    }

    /**
     * Updates the selectivity factor according to the given node.
     *
     * @param node leaf node
     */
    void visit(FilterNode node) {
        if (node instanceof FilterAndProjectTemporalVerticesNode) {
            FilterAndProjectTemporalVerticesNode vertexNode =
                    (FilterAndProjectTemporalVerticesNode) node;
            setCardinality(vertexNode.getEmbeddingMetaData().getVertexVariables().get(0), true);
            updateSelectivity(vertexNode.getFilterPredicate());
        } else if (node instanceof FilterAndProjectTemporalEdgesNode) {
            FilterAndProjectTemporalEdgesNode edgeNode = (FilterAndProjectTemporalEdgesNode) node;
            setCardinality(edgeNode.getEmbeddingMetaData().getEdgeVariables().get(0), false);
            updateSelectivity(edgeNode.getFilterPredicate());
        } else if (node instanceof FilterTemporalEmbeddingsNode) {
            updateSelectivity(((FilterTemporalEmbeddingsNode) node).getFilterPredicate());
        }
    }

    /**
     * Returns the non-filtered cardinality of the leaf node output.
     *
     * @return estimated cardinality
     */
    long getCardinality() {
        return cardinality;
    }

    /**
     * Returns the combined selectivity of all leaf nodes.
     *
     * @return combined selectivity factor
     */
    double getSelectivity() {
        return selectivity;
    }

    /**
     * Updates the cardinality of the leaf node output.
     *
     * @param variable query variable
     * @param isVertex true, iff the variable maps to a vertex
     */
    private void setCardinality(String variable, boolean isVertex) {
        cardinality = getCardinality(getLabel(variable, isVertex), isVertex);
    }

    /**
     * Updates the selectivity based on the given predicates.
     *
     * @param predicates query predicates
     */
    private void updateSelectivity(TemporalCNF predicates) {
        selectivity *= cnfEstimation.estimateCNF(predicates);
    }

    public CNFEstimation getCnfEstimation(){
        return cnfEstimation;
    }
}
