package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.binary;


import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.JoinNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.ExpandEmbeddingsTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.ExpandEmbeddingsTPGMBulk;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpansionCriteria;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.BinaryNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Binary node that wraps an {@link ExpandEmbeddingsTPGMBulk} operator.
 */
public class ExpandEmbeddingsTPGMNode extends BinaryNode implements JoinNode {
    /**
     * Column to expand the embedding from.
     */
    private final int expandColumn;
    /**
     * Column in which the expand vertex's time data is stored
     */
    private final int expandVertexTimeColumn;
    /**
     * Query variable of the first vertex in the path
     */
    private final String startVariable;
    /**
     * Query variable of the variable length path
     */
    private final String pathVariable;
    /**
     * Query variable of the last vertex in the path
     */
    private final String endVariable;
    /**
     * Minimum number of path expansion steps
     */
    private final int lowerBound;
    /**
     * Maximum number of path expansion steps
     */
    private final int upperBound;
    /**
     * Column that contains the final vertex of the expansion
     */
    private final int closingColumn;
    /**
     * Direction in which to expand the embedding
     */
    private final ExpandDirection expandDirection;
    /**
     * Morphism type for vertices
     */
    private final MatchStrategy vertexStrategy;
    /**
     * Morphism type for edges
     */
    private final MatchStrategy edgeStrategy;
    /**
     * Conditions for the path expansion
     */
    private ExpansionCriteria criteria;

    public ExpandEmbeddingsTPGMNode(PlanNode leftChild, PlanNode rightChild,
                                String startVariable, String pathVariable, String endVariable,
                                int lowerBound, int upperBound, ExpandDirection expandDirection,
                                MatchStrategy vertexStrategy, MatchStrategy edgeStrategy, ExpansionCriteria criteria) {
        super(leftChild, rightChild);
        this.pathVariable = pathVariable;
        this.startVariable = startVariable;
        this.endVariable = endVariable;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound == 0 ? Integer.MAX_VALUE : upperBound;
        this.expandDirection = expandDirection;
        this.vertexStrategy = vertexStrategy;
        this.edgeStrategy = edgeStrategy;
        this.expandColumn = leftChild.getEmbeddingMetaData().getEntryColumn(startVariable);
        this.expandVertexTimeColumn = leftChild.getEmbeddingMetaData().getTimeColumn(startVariable);
        this.closingColumn = leftChild.getEmbeddingMetaData().containsEntryColumn(endVariable) ?
                leftChild.getEmbeddingMetaData().getEntryColumn(endVariable) : -1;
        this.criteria = criteria;
    }

    @Override
    public DataSet<EmbeddingTPGM> execute() {
        ExpandEmbeddingsTPGM op = new ExpandEmbeddingsTPGMBulk(
                getLeftChild().execute(), getRightChild().execute(),
                expandColumn, expandVertexTimeColumn, lowerBound, upperBound, expandDirection,
                getDistinctVertexColumns(getLeftChild().getEmbeddingMetaData()),
                getDistinctEdgeColumns(getLeftChild().getEmbeddingMetaData()),
                closingColumn, JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES, criteria);
        op.setName(toString());
        return op.evaluate();
    }

    @Override
    protected EmbeddingTPGMMetaData computeEmbeddingMetaData() {
        EmbeddingTPGMMetaData inputMetaData = getLeftChild().getEmbeddingMetaData();
        EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData(inputMetaData);

        metaData.setEntryColumn(pathVariable, EmbeddingTPGMMetaData.EntryType.PATH,
                inputMetaData.getEntryCount());

        metaData.setDirection(pathVariable, expandDirection);

        if (!inputMetaData.containsEntryColumn(endVariable)) {
            metaData.setEntryColumn(endVariable, EmbeddingTPGMMetaData.EntryType.VERTEX,
                    inputMetaData.getEntryCount() + 1);
            metaData.setTimeColumn(endVariable, inputMetaData.getTimeCount());
        }
        return metaData;
    }


    /**
     * According to the specified {@link #vertexStrategy} and the specified
     * {@link EmbeddingTPGMMetaData}, the method returns the columns that need to contain distinct
     * entries.
     *
     * @param metaData meta data for the embedding
     * @return distinct vertex columns
     */
    private List<Integer> getDistinctVertexColumns(EmbeddingTPGMMetaData metaData) {
        return this.vertexStrategy == MatchStrategy.ISOMORPHISM ? metaData.getVertexVariables().stream()
                .map(metaData::getEntryColumn)
                .collect(Collectors.toList()) : Collections.emptyList();
    }

    /**
     * According to the specified {@link #edgeStrategy} and the specified
     * {@link EmbeddingTPGMMetaData}, the method returns the columns that need to contain distinct
     * entries.
     *
     * @param metaData meta data for the embedding
     * @return distinct edge columns
     */
    private List<Integer> getDistinctEdgeColumns(EmbeddingTPGMMetaData metaData) {
        return edgeStrategy == MatchStrategy.ISOMORPHISM ?
                metaData.getEdgeVariables().stream()
                        .map(metaData::getEntryColumn)
                        .collect(Collectors.toList()) : Collections.emptyList();
    }

    @Override
    public String toString() {
        return String.format("ExpandEmbeddingsNode={" +
                        "startVariable='%s', " +
                        "pathVariable='%s', " +
                        "endVariable='%s', " +
                        "lowerBound=%d, " +
                        "upperBound=%d, " +
                        "expandDirection=%s, " +
                        "vertexMorphismType=%s, " +
                        "edgeMorphismType=%s}",
                startVariable, pathVariable, endVariable, lowerBound, upperBound, expandDirection,
                vertexStrategy, edgeStrategy);
    }
}
