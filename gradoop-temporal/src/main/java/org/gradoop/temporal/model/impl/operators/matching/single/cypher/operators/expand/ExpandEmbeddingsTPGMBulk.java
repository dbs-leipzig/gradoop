package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand;

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions.*;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpandEmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpansionCriteria;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

import java.util.List;

/**
 * Expands a vertex along the edges. The number of hops can be specified via upper and lower bound
 * The input embedding is appended by 2 Entries, the first one represents the path (edge, vertex,
 * edge, vertex, ..., edge), the second one the end vertex
 * Furthermore, temporal data of the end vertex is appended.
 * Temporal conditions for expanding the path can be provided by {@link ExpansionCriteria}
 *
 * Iteration is done with {@code BulkIteration}
 */
public class ExpandEmbeddingsTPGMBulk extends ExpandEmbeddingsTPGM{

    /**
     * New Expand One Operator
     *
     * @param input the embedding which should be expanded
     * @param candidateEdges candidate edges along which we expand
     * @param expandColumn specifies the input column that represents the vertex from which we expand
     * @param expandVertexTimeColumn time column where the expand vertex's time data is stored
     * @param lowerBound specifies the minimum hops we want to expand
     * @param upperBound specifies the maximum hops we want to expand
     * @param direction direction of the expansion (see {@link ExpandDirection})
     * @param distinctVertexColumns indices of distinct input vertex columns
     * @param distinctEdgeColumns indices of distinct input edge columns
     * @param closingColumn defines the column which should be equal with the paths end
     * @param joinHint join strategy
     * @param criteria temporal expansion conditions
     */
    public ExpandEmbeddingsTPGMBulk(DataSet<EmbeddingTPGM> input, DataSet<EmbeddingTPGM> candidateEdges,
                                    int expandColumn, int expandVertexTimeColumn, int lowerBound, int upperBound, ExpandDirection direction,
                                    List<Integer> distinctVertexColumns, List<Integer> distinctEdgeColumns, int closingColumn,
                                    JoinOperatorBase.JoinHint joinHint, ExpansionCriteria criteria) {

        super(input, candidateEdges, expandColumn, expandVertexTimeColumn, lowerBound, upperBound, direction,
                distinctVertexColumns, distinctEdgeColumns, closingColumn, joinHint, criteria);
    }

    /**
     * New Expand One Operator with default join strategy
     *
     * @param input the embedding which should be expanded
     * @param candidateEdges candidate edges along which we expand
     * @param expandColumn specifies the column that represents the vertex from which we expand
     * @param expandVertexTimeColumn time column where the expand vertex's time data is stored
     * @param lowerBound specifies the minimum hops we want to expand
     * @param upperBound specifies the maximum hops we want to expand
     * @param direction direction of the expansion (see {@link ExpandDirection})
     * @param distinctVertexColumns indices of distinct vertex columns
     * @param distinctEdgeColumns indices of distinct edge columns
     * @param closingColumn defines the column which should be equal with the paths end
     */
    public ExpandEmbeddingsTPGMBulk(DataSet<EmbeddingTPGM> input, DataSet<EmbeddingTPGM> candidateEdges,
                                int expandColumn, int expandVertexTimeColumn, int lowerBound, int upperBound, ExpandDirection direction,
                                List<Integer> distinctVertexColumns, List<Integer> distinctEdgeColumns, int closingColumn,
                                ExpansionCriteria criteria) {

        this(input, candidateEdges, expandColumn, expandVertexTimeColumn, lowerBound, upperBound, direction,
                distinctVertexColumns, distinctEdgeColumns, closingColumn,
                JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES, criteria);
    }

    /**
     * New Expand One Operator with no upper bound
     *
     * @param input the embedding which should be expanded
     * @param candidateEdges candidate edges along which we expand
     * @param expandColumn specifies the column that represents the vertex from which we expand
     * @param expandVertexTimeColumn time column where the expand vertex's time data is stored
     * @param lowerBound specifies the minimum hops we want to expand
     * @param direction direction of the expansion (see {@link ExpandDirection})
     * @param distinctVertexColumns indices of distinct vertex columns
     * @param distinctEdgeColumns indices of distinct edge columns
     * @param closingColumn defines the column which should be equal with the paths end
     */
    public ExpandEmbeddingsTPGMBulk(DataSet<EmbeddingTPGM> input, DataSet<EmbeddingTPGM> candidateEdges,
                                int expandColumn, int expandVertexTimeColumn, int lowerBound, ExpandDirection direction,
                                List<Integer> distinctVertexColumns, List<Integer> distinctEdgeColumns, int closingColumn,
                                ExpansionCriteria criteria) {

        this(input, candidateEdges, expandColumn, expandVertexTimeColumn, lowerBound, Integer.MAX_VALUE, direction,
                distinctVertexColumns, distinctEdgeColumns, closingColumn,
                JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES, criteria);
    }

    @Override
    protected DataSet<ExpandEmbeddingTPGM> iterate(DataSet<ExpandEmbeddingTPGM> initialWorkingSet) {

        IterativeDataSet<ExpandEmbeddingTPGM> iteration = initialWorkingSet
                .iterate(upperBound - 1)
                .name(getName());

        DataSet<ExpandEmbeddingTPGM> nextWorkingSet = iteration
                .filter(new FilterPreviousExpandEmbeddingTPGM())
                .name(getName() + " - FilterRecent")
                .join(candidateEdgeTuples, joinHint)
                .where(new ExtractEmbeddingEndID())
                .equalTo(new ExtractEdgeStartID())
                .with(new MergeExpandEmbeddingsTPGM(
                        distinctVertexColumns,
                        distinctEdgeColumns,
                        closingColumn,
                        criteria
                ))
                .name(getName() + " - Expansion");

        DataSet<ExpandEmbeddingTPGM> solutionSet = nextWorkingSet.union(iteration);

        return iteration.closeWith(solutionSet, nextWorkingSet);
    }


}
