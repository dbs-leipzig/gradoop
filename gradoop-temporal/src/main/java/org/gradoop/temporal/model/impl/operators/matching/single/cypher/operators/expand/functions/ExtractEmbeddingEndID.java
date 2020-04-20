package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpandEmbeddingTPGM;

/**
 * Extracts the Gradoop ID of an ExpandEmbeddings last vertex (vertex used to join)
 */
public class ExtractEmbeddingEndID implements KeySelector<ExpandEmbeddingTPGM, GradoopId> {
    @Override
    public GradoopId getKey(ExpandEmbeddingTPGM value) throws Exception {
        return value.getEndVertex();
    }
}
