package org.gradoop.temporal.model.impl.operators.matching.single.cypher.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

/**
 * Reverses an edge embedding, as it switches source and target ids and timedata
 * This is used for traversing incoming edges
 */
public class ReverseEdgeEmbeddingTPGM extends RichMapFunction<EmbeddingTPGM, EmbeddingTPGM> {
    @Override
    public EmbeddingTPGM map(EmbeddingTPGM value) throws Exception {
        return value.reverse();
    }
}
