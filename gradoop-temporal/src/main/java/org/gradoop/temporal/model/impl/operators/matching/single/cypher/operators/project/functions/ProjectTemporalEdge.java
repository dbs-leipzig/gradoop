package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMFactory;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

import java.util.List;

/**
 * Projects an Edge by a set of properties.
 * <p>
 * {@code TPGM Edge -> EmbeddingTPGM(GraphElementEmbedding(Edge))}
 */
public class ProjectTemporalEdge extends RichMapFunction<TemporalEdge, EmbeddingTPGM> {
    /**
     * Names of the properties that will be kept in the projection
     */
    private final List<String> propertyKeys;
    /**
     * Indicates if the edges is a loop
     */
    private final boolean isLoop;

    /**
     * Creates a new edge projection function
     *
     * @param propertyKeys the property keys that will be kept
     * @param isLoop indicates if edges is a loop
     */
    public ProjectTemporalEdge(List<String> propertyKeys, boolean isLoop) {
        this.propertyKeys = propertyKeys;
        this.isLoop = isLoop;
    }

    @Override
    public EmbeddingTPGM map(TemporalEdge edge) {
        return EmbeddingTPGMFactory.fromEdge(edge, propertyKeys, isLoop);
    }
}
