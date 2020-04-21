package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan;

import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;

import java.util.List;
import java.util.stream.IntStream;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

/**
 * Represents a leaf node in the query plan. Leaf nodes are different in terms of their input which
 * is a data set containing elements, i.e. {@link TemporalVertex} or
 * {@link TemporalEdge}.
 */
public abstract class LeafNode extends PlanNode{
    /**
     * Sets the property columns in the specified meta data object according to the specified variable
     * and property keys.
     *
     * @param metaData meta data to update
     * @param variable variable to associate properties to
     * @param propertyKeys properties needed for filtering and projection
     * @return updated EmbeddingMetaData
     */
    protected EmbeddingTPGMMetaData setPropertyColumns(EmbeddingTPGMMetaData metaData, String variable,
                                                       List<String> propertyKeys) {
        IntStream.range(0, propertyKeys.size())
                .forEach(i -> metaData.setPropertyColumn(variable, propertyKeys.get(i), i));
        return metaData;
    }
}
