package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.leaf;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.FilterNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.ProjectionNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpansionCriteria;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.FilterAndProjectTemporalEdges;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.LeafNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Leaf node that wraps a {@link FilterAndProjectTemporalEdges} operator.
 */
public class FilterAndProjectTemporalEdgesNode extends LeafNode
implements FilterNode, ProjectionNode {
    /**
     * Input data set
     */
    private DataSet<TemporalEdge> edges;
    /**
     * Query variable of the source vertex
     */
    private final String sourceVariable;
    /**
     * Query variable of the edge
     */
    private final String edgeVariable;
    /**
     * Query variable of the target vertex
     */
    private final String targetVariable;
    /**
     * Filter predicate that is applied on the input data set
     */
    private CNF filterPredicate;
    /**
     * Property keys used for projection
     */
    private final List<String> projectionKeys;
    /**
     * Indicates if the edges is actually a path
     */
    private final boolean isPath;

    /**
     * Creates a new node.
     *
     * @param edges input edges
     * @param sourceVariable query variable of the source vertex
     * @param edgeVariable query variable of the edge
     * @param targetVariable query variable of the target vertex
     * @param filterPredicate filter predicate to be applied on edges
     * @param projectionKeys property keys whose associated values are projected to the output
     * @param isPath indicates if the edges is actually a path
     */
    public FilterAndProjectTemporalEdgesNode(DataSet<TemporalEdge> edges,
                                             String sourceVariable, String edgeVariable, String targetVariable,
                                             CNF filterPredicate, Set<String> projectionKeys, boolean isPath) {
        this.edges = edges;
        this.sourceVariable = sourceVariable;
        this.edgeVariable = edgeVariable;
        this.targetVariable = targetVariable;
        this.filterPredicate = filterPredicate;
        this.projectionKeys = new ArrayList<>(projectionKeys);
        this.isPath = isPath;
    }

    @Override
    public DataSet<EmbeddingTPGM> execute() {
        FilterAndProjectTemporalEdges op =  new FilterAndProjectTemporalEdges(
                edges,
                filterPredicate,
                projectionKeys,
                isLoop()
        );
        op.setName(toString());
        return op.evaluate();
    }

    /**
     * Returns a copy of the filter predicate attached to this node.
     *
     * @return filter predicate
     */
    public CNF getFilterPredicate() {
        return new CNF(filterPredicate);
    }

    /**
     * Returns a copy of the projection keys attached to this node.
     *
     * @return projection keys
     */
    public List<String> getProjectionKeys() {
        return new ArrayList<>(projectionKeys);
    }

    /**
     * checks whether edge is a self-loop
     * @return true iff edge is a self-loop
     */
    public boolean isLoop() {
        return sourceVariable.equals(targetVariable) && !isPath;
    }

    @Override
    protected EmbeddingTPGMMetaData computeEmbeddingMetaData() {
        EmbeddingTPGMMetaData embeddingMetaData = new EmbeddingTPGMMetaData();
        embeddingMetaData.setEntryColumn(sourceVariable, EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        embeddingMetaData.setEntryColumn(edgeVariable, EmbeddingTPGMMetaData.EntryType.EDGE, 1);
        if (!isLoop()) {
            embeddingMetaData.setEntryColumn(targetVariable, EmbeddingTPGMMetaData.EntryType.VERTEX, 2);
        }

        embeddingMetaData = setPropertyColumns(embeddingMetaData, edgeVariable, projectionKeys);
        embeddingMetaData.setTimeColumn(edgeVariable, 0);

        return embeddingMetaData;
    }

    @Override
    public String toString() {
        return String.format("FilterAndProjectEdgesNode{" +
                        "sourceVariable='%s', " +
                        "edgeVariable='%s', " +
                        "targetVariable='%s', " +
                        "filterPredicate=%s, " +
                        "projectionKeys=%s}",
                sourceVariable, edgeVariable, targetVariable, filterPredicate, projectionKeys);
    }

}
