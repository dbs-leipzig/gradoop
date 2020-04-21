package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.leaf;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.FilterNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.ProjectionNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.FilterAndProjectTemporalVertices;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.LeafNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class FilterAndProjectTemporalVerticesNode extends LeafNode
        implements FilterNode, ProjectionNode {

    /**
     * Input data set
     */
    private DataSet<TemporalVertex> vertices;
    /**
     * Query variable of the vertex
     */
    private final String vertexVariable;
    /**
     * Filter predicate that is applied on the input data set
     */
    private CNF filterPredicate;
    /**
     * Property keys used for projection
     */
    private final List<String> projectionKeys;
    /**
     * Creates a new node.
     *
     * @param vertices input vertices
     * @param vertexVariable query variable of the vertex
     * @param filterPredicate filter predicate to be applied on edges
     * @param projectionKeys property keys whose associated values are projected to the output
     */
    public FilterAndProjectTemporalVerticesNode(DataSet<TemporalVertex> vertices, String vertexVariable,
                                        CNF filterPredicate, Set<String> projectionKeys) {
        this.vertices = vertices;
        this.vertexVariable = vertexVariable;
        this.filterPredicate = filterPredicate;
        this.projectionKeys = new ArrayList<>(projectionKeys);
    }

    @Override
    public DataSet<EmbeddingTPGM> execute() {
        FilterAndProjectTemporalVertices op =
                new FilterAndProjectTemporalVertices(vertices, filterPredicate, projectionKeys);
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

    @Override
    protected EmbeddingTPGMMetaData computeEmbeddingMetaData() {
        EmbeddingTPGMMetaData embeddingMetaData = new EmbeddingTPGMMetaData();
        embeddingMetaData.setEntryColumn(vertexVariable, EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        embeddingMetaData = setPropertyColumns(embeddingMetaData, vertexVariable, projectionKeys);
        embeddingMetaData.setTimeColumn(vertexVariable, 0);

        return embeddingMetaData;
    }

    @Override
    public String toString() {
        return String.format("FilterAndProjectVerticesNode{" +
                        "vertexVariable=%s, " +
                        "filterPredicate=%s, " +
                        "projectionKeys=%s}",
                vertexVariable, filterPredicate, projectionKeys);
    }

}
