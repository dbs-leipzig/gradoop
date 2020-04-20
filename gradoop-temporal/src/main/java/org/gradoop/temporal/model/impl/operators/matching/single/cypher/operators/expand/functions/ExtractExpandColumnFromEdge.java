package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

/**
 * Extracts a join key from an id stored in an embedding record
 * The id is referenced via its column index.
 */
public class ExtractExpandColumnFromEdge implements KeySelector<EmbeddingTPGM, GradoopId> {
    /**
     * Column that holds the id which will be used as key
     */
    private final Integer column;

    /**
     * Creates the key selector
     *
     * @param column column that holds the id which will be used as key
     */
    public ExtractExpandColumnFromEdge(Integer column) {
        this.column = column;
    }

    @Override
    public GradoopId getKey(EmbeddingTPGM value) throws Exception {
        return value.getId(column);
    }
}
