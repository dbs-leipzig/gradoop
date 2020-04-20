package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand;

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions.*;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.functions.ReverseEdgeEmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpandEmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpansionCriteria;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.TemporalEdgeWithTiePoint;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

import java.util.List;

/**
 * Expands a vertex along the edges. The number of hops can be specified via upper and lower bound
 * The input embedding is appended by 2 Entries, the first one represents the path (edge, vertex,
 * edge, vertex, ..., edge), the second one the end vertex.
 * Furthermore, temporal constraints on the path can be given.
 */
public abstract class ExpandEmbeddingsTPGM implements PhysicalTPGMOperator {

    /**
     * Input Embeddings
     */
    protected final DataSet<EmbeddingTPGM> input;
    /**
     * specifies the input column that will be expanded
     */
    protected final int expandColumn;
    /**
     * specifies the column in which the expand vertex's time data is stored
     */
    protected final int expandVertexTimeColumn;
    /**
     * minimum hops
     */
    protected final int lowerBound;
    /**
     * maximum hops
     */
    protected final int upperBound;
    /**
     * expand direction
     */
    protected final ExpandDirection direction;
    /**
     * Holds indices of input vertex columns that should be distinct
     */
    protected final List<Integer> distinctVertexColumns;
    /**
     * Holds indices of input edge columns that should be distinct
     */
    protected final List<Integer> distinctEdgeColumns;
    /**
     * Define the column which should be equal with the paths end
     */
    protected final int closingColumn;
    /**
     * join hint
     */
    protected final JoinOperatorBase.JoinHint joinHint;
    /**
     * Candidate edges
     */
    protected DataSet<EmbeddingTPGM> candidateEdges;
    /**
     * candidate edges with extracted map key
     */
    protected DataSet<TemporalEdgeWithTiePoint> candidateEdgeTuples;
    /**
     * Temporal constraints on the path
     */
    protected final ExpansionCriteria criteria;

    /**
     * Operator name used for Flink operator description
     */
    protected String name;

    /**
     * New Expand operator
     *
     * @param input the embedding which should be expanded
     * @param expandColumn specifies the input column that represents the vertex from which we expand
     * @param expandVertexTimeColumn time column where time data for start vertex is stored
     * @param lowerBound specifies the minimum hops we want to expand
     * @param upperBound specifies the maximum hops we want to expand
     * @param direction direction of the expansion (see {@link ExpandDirection})
     * @param distinctVertexColumns indices of distinct input vertex columns
     * @param distinctEdgeColumns indices of distinct input edge columns
     * @param closingColumn defines the column which should be equal with the paths end
     * @param joinHint join strategy
     * @param criteria temporal expansion conditions
     */
    public ExpandEmbeddingsTPGM(DataSet<EmbeddingTPGM> input, DataSet<EmbeddingTPGM> candidateEdges,
                                int expandColumn, int expandVertexTimeColumn,
                                int lowerBound, int upperBound, ExpandDirection direction,
                                List<Integer> distinctVertexColumns,
                                List<Integer> distinctEdgeColumns, int closingColumn,
                                JoinOperatorBase.JoinHint joinHint,
                                ExpansionCriteria criteria) {
        this.input = input;
        this.candidateEdges = candidateEdges;
        this.expandColumn = expandColumn;
        this.expandVertexTimeColumn = expandVertexTimeColumn;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.direction = direction;
        this.distinctVertexColumns = distinctVertexColumns;
        this.distinctEdgeColumns = distinctEdgeColumns;
        this.closingColumn = closingColumn;
        this.joinHint = joinHint;
        this.criteria = criteria;
        this.name = "ExpandEmbeddings";
    }

    /**
     * Runs a traversal over the given edgeCandidates withing the given bounds
     *
     * @return the input appened by 2 entries (IdList(Path), IdEntry(End Vertex)
     */
    @Override
    public DataSet<EmbeddingTPGM> evaluate() {
        DataSet<ExpandEmbeddingTPGM> initialWorkingSet = preProcess();

        DataSet<ExpandEmbeddingTPGM> iterationResults = iterate(initialWorkingSet);

        return postProcess(iterationResults);
    }

    /**
     * Runs the iterative traversal
     *
     * @param initialWorkingSet the initial edges which are used as starting points for the traversal
     * @return set of paths produced by the iteration (length 1..upperBound)
     */
    protected abstract DataSet<ExpandEmbeddingTPGM> iterate(DataSet<ExpandEmbeddingTPGM> initialWorkingSet);

    /**
     * creates the initial working set from the edge candidates
     *
     * @return initial working set with the expand embeddings
     */
    private DataSet<ExpandEmbeddingTPGM> preProcess() {
        if (direction == ExpandDirection.IN) {
            candidateEdges = candidateEdges
                    .map(new ReverseEdgeEmbeddingTPGM())
                    .name(getName() + " - Reverse Edges");
        }

        this.candidateEdgeTuples = candidateEdges
                .map(new ExtractKeyedCandidateEdges())
                .name(getName() + " - Create candidate edge tuples")
                .partitionByHash(new ExtractEdgeStartID())
                .name(getName() + " - Partition edge tuples");

        return input.join(candidateEdgeTuples, joinHint)
                .where(new ExtractExpandColumnFromEdge(expandColumn)).equalTo(new ExtractEdgeStartID())
                .with(new CreateExpandEmbeddingTPGM(
                        distinctVertexColumns,
                        distinctEdgeColumns,
                        closingColumn,
                        criteria
                ))
                .name(getName() + " - Initial expansion");
    }

    /**
     * Produces the final operator results from the iteration results
     *
     * @param iterationResults the results produced by the iteration
     * @return iteration results filtered by upper and lower bound and combined with input data
     */
    private DataSet<EmbeddingTPGM> postProcess(DataSet<ExpandEmbeddingTPGM> iterationResults) {
        DataSet<EmbeddingTPGM> results = iterationResults
                .flatMap(new PostProcessExpandEmbeddingTPGM(lowerBound, closingColumn))
                .name(getName() + " - Post Processing");

        if (lowerBound == 0) {
            results = results.union(
                    input
                            .flatMap(new AdoptEmptyPaths(expandColumn, expandVertexTimeColumn,
                                    closingColumn))
                            .name(getName() + " - Append empty paths")
            );
        }

        return results;
    }

    @Override
    public void setName(String newName) {
        this.name = newName;
    }

    @Override
    public String getName() {
        return this.name;
    }
}
