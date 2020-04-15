package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.List;
/**
 * Projects a TPGM Vertex by a set of properties.
 * <p>
 * {@code TemporalVertex -> EmbeddingTPGM(GraphElementEmbedding(Vertex))}
 */
public class ProjectTemporalVertex extends RichMapFunction<TemporalVertex, EmbeddingTPGM> {
    /**
     * Names of the properties that will be kept in the projection
     */
    private final List<String> propertyKeys;

    /**
     * Creates a new vertex projection function
     *
     * @param propertyKeys List of propertyKeys that will be kept in the projection
     */
    public ProjectTemporalVertex(List<String> propertyKeys) {
        this.propertyKeys = propertyKeys;
    }

    @Override
    public EmbeddingTPGM map(TemporalVertex vertex) {
        return EmbeddingTPGMFactory.fromVertex(vertex, propertyKeys);
    }
}
