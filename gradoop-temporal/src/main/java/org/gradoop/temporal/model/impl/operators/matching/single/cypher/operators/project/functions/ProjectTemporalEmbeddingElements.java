package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

import java.util.Map;

/**
 * Projects {@link EmbeddingTPGM} entry columns based on a column-to-column mapping
 */
public class ProjectTemporalEmbeddingElements implements MapFunction<EmbeddingTPGM, EmbeddingTPGM> {
    /**
     * A mapping from input column to output column
     */
    private final Map<Integer, Integer> projectionColumns;
    /**
     * New embeddings filter function
     *
     * @param projectionColumns Variables to keep in output embedding
     */
    public ProjectTemporalEmbeddingElements(Map<Integer, Integer> projectionColumns) {
        this.projectionColumns = projectionColumns;
    }

    @Override
    public EmbeddingTPGM map(EmbeddingTPGM embedding) throws Exception {
        GradoopId[] idField = new GradoopId[projectionColumns.size()];

        for (Map.Entry<Integer, Integer> projection : projectionColumns.entrySet()) {
            idField[projection.getValue()] = embedding.getId(projection.getKey());
        }

        EmbeddingTPGM newEmbedding = new EmbeddingTPGM();
        newEmbedding.addAll(idField);
        return newEmbedding;
    }
}
