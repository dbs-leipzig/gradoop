package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpandEmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpansionCriteria;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.TemporalEdgeWithTiePoint;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

import java.util.List;

/**
 * Creates the initial expand embeddings
 */
public class CreateExpandEmbeddingTPGM extends
        RichFlatJoinFunction<EmbeddingTPGM, TemporalEdgeWithTiePoint, ExpandEmbeddingTPGM> {

    /**
     * Holds the index of all base vertex columns that should be distinct
     */
    private final List<Integer> distinctVertices;
    /**
     * Holds the index of all base edge columns that should be distinct
     */
    private final List<Integer> distinctEdges;
    /**
     * Specifies a base column that should be equal to the paths end node
     */
    private final int closingColumn;
    /**
     * Specifies temporal conditions the expanded path must fulfill
     */
    private final ExpansionCriteria expansionCriteria;

    /**
     * Creates new FlatJoin function
     * @param distinctVertices indices of distinct vertex columns
     * @param distinctEdges indices of distinct edge columns
     * @param closingColumn base column that should be equal to a paths end node
     * @param expansionCriteria describes temporal conditions the expanded path must fulfill
     */
    public CreateExpandEmbeddingTPGM(List<Integer> distinctVertices,
                                     List<Integer> distinctEdges, int closingColumn,
                                     ExpansionCriteria expansionCriteria) {
        this.distinctVertices = distinctVertices;
        this.distinctEdges = distinctEdges;
        this.closingColumn = closingColumn;
        this.expansionCriteria = expansionCriteria;
    }

    @Override
    public void join(EmbeddingTPGM input, TemporalEdgeWithTiePoint edge,
                     Collector<ExpandEmbeddingTPGM> out)
            throws Exception {

        if (checkDistinctiveness(input, edge) && expansionCriteria.checkInitialEdge(edge)) {
            out.collect(createInitialEmbedding(input, edge));
        }
    }

    /**
     * Builds an ExpandEmbedding whose expanded path consists only of one edge.
     * @param input the EmbeddingTPGM on which the ExpandEmbedding should be based
     * @param edge the edge to expand the initial EmbeddingTPGM
     * @return ExpandEmbedding based on the EmbeddingTPGM. Its expanded path consists only
     * of the edge.
     */
    private ExpandEmbeddingTPGM createInitialEmbedding(EmbeddingTPGM input,
                                                       TemporalEdgeWithTiePoint edge){
        GradoopId[] path = new GradoopId[]{edge.getEdge(), edge.getTarget()};
        Long[] sourceTimeData = edge.getSourceTimeData();
        Long[] edgeTimeData = edge.getEdgeTimeData();
        Long[] targetTimeData = edge.getTargetTimeData();
        Long maxTxFrom = Math.max(Math.max(sourceTimeData[0], edgeTimeData[0]), targetTimeData[0]);
        Long minTxTo = Math.min(Math.min(sourceTimeData[1], edgeTimeData[1]), targetTimeData[1]);
        Long maxValidFrom = Math.max(Math.max(sourceTimeData[2], edgeTimeData[2]), targetTimeData[2]);
        Long minValidTo = Math.min(Math.min(sourceTimeData[3], edgeTimeData[3]), targetTimeData[3]);
        return new ExpandEmbeddingTPGM(input, path, edgeTimeData, targetTimeData, maxTxFrom, minTxTo,
                maxValidFrom, minValidTo);
    }

    /**
     * Checks the distinct criteria for the expansion
     * @param input the base part of the expansion
     * @param edge edge along which we expand
     * @return true if distinct criteria hold for the expansion
     */
    private boolean checkDistinctiveness(EmbeddingTPGM input, TemporalEdgeWithTiePoint edge) {
        GradoopId edgeId = edge.getEdge();
        GradoopId tgt = edge.getTarget();

        for (int i : distinctVertices) {
            if (input.getIdAsList(i).contains(tgt) && i != closingColumn) {
                return false;
            }
        }

        for (int i : distinctEdges) {
            if (input.getIdAsList(i).contains(edgeId)) {
                return false;
            }
        }

        return true;
    }
}
