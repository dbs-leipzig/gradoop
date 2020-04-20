package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.add.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

/**
 * Adds {@link EmbeddingTPGM} columns based on a given set of variables.
 */
public class AddEmbeddingTPGMElements extends RichMapFunction<EmbeddingTPGM, EmbeddingTPGM> {
    /**
     * Return pattern variables that do not exist in pattern matching query
     */
    private final int count;

    /**
     * Creates a new function
     *
     * @param count number of elements to add
     */
    public AddEmbeddingTPGMElements(int count) {
        this.count = count;
    }

    @Override
    public EmbeddingTPGM map(EmbeddingTPGM embedding) {
        for (int i = 0; i < count; i++) {
            GradoopId id = GradoopId.get();
            embedding.add(id);
        }
        return embedding;
    }
}
