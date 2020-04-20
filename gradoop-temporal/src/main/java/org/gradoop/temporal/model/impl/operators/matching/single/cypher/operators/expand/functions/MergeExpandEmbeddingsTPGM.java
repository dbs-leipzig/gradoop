package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpandEmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpansionCriteria;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.TemporalEdgeWithTiePoint;

import java.util.List;

/**
 * Combines the results of a join between intermediate result and an edge embedding by growing
 * the intermediate result.
 * Before growing it is checked whether distinctiveness and temporal conditions would still apply.
 */
public class MergeExpandEmbeddingsTPGM extends RichFlatJoinFunction<ExpandEmbeddingTPGM,
        TemporalEdgeWithTiePoint, ExpandEmbeddingTPGM> {

    /**
     * Holds the index of all vertex columns that should be distinct
     */
    private final List<Integer> distinctVertices;
    /**
     * Holds the index of all edge columns that should be distinct
     */
    private final List<Integer> distinctEdges;
    /**
     * Specifies a base column that should be equal to the paths end node
     */
    private final int closingColumn;
    /**
     * Specifies temporal conditions that must hold on the path
     */
    private final ExpansionCriteria criteria;

    /**
     * Create a new merge operator
     * @param distinctVertices distinct vertex columns
     * @param distinctEdges distinct edge columns
     * @param closingColumn base column that should be equal to a paths end node
     * @param criteria temporal conditions that must hold on the path
     */
    public MergeExpandEmbeddingsTPGM(List<Integer> distinctVertices,
                                     List<Integer> distinctEdges, int closingColumn,
                                     ExpansionCriteria criteria) {
        this.distinctVertices = distinctVertices;
        this.distinctEdges = distinctEdges;
        this.closingColumn = closingColumn;
        this.criteria = criteria;
    }

    @Override
    public void join(ExpandEmbeddingTPGM base, TemporalEdgeWithTiePoint edge,
                     Collector<ExpandEmbeddingTPGM> out) throws Exception {

        if (checkDistinctiveness(base, edge) && criteria.checkExpansion(base, edge)) {
            out.collect(base.grow(edge));
        }
    }

    /**
     * Checks the distinctiveness criteria for the expansion
     * @param prev previous intermediate result
     * @param edge edge along which we expand
     * @return true if distinct criteria apply for the expansion
     */
    private boolean checkDistinctiveness(ExpandEmbeddingTPGM prev, TemporalEdgeWithTiePoint edge) {
        if (distinctVertices.isEmpty() && distinctEdges.isEmpty()) {
            return true;
        }

        // the new candidate is invalid under vertex isomorphism
        if (edge.getSource().equals(edge.getTarget()) &&
                !distinctVertices.isEmpty()) {
            return false;
        }

        // check if there are any clashes in the path
        for (GradoopId ref : prev.getPath()) {
            if ((ref.equals(edge.getSource()) || ref.equals(edge.getTarget()) &&
                    !distinctVertices.isEmpty()) || (ref.equals(edge.getEdge()) && !distinctEdges.isEmpty())) {
                return false;
            }
        }

        List<GradoopId> ref;

        // check for clashes with distinct vertices in the base
        for (int i : distinctVertices) {
            ref = prev.getBase().getIdAsList(i);
            if ((ref.contains(edge.getTarget()) && i != closingColumn) ||
                    ref.contains(edge.getSource())) {
                return false;
            }
        }

        // check for clashes with distinct edges in the base
        ref = prev.getBase().getIdsAsList(distinctEdges);
        return !ref.contains(edge.getEdge());
    }
}
