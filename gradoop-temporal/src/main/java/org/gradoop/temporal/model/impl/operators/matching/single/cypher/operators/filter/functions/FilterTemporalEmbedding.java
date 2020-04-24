package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;

/**
 * Filters a set of temporal embeddings by given predicates
 */
public class FilterTemporalEmbedding extends RichFilterFunction<EmbeddingTPGM> {
    /**
     * Predicates used for filtering
     */
    private final CNF predicates;
    /**
     * Mapping of variables names to embedding column
     */
    private final EmbeddingTPGMMetaData metaData;

    /**
     * New embedding filter function
     *
     * @param predicates predicates used for filtering
     * @param metaData mapping of variable names to embedding column
     */
    public FilterTemporalEmbedding(CNF predicates, EmbeddingTPGMMetaData metaData) {
        this.predicates = predicates;
        this.metaData = metaData;
    }

    @Override
    public boolean filter(EmbeddingTPGM embedding) {
        return predicates.evaluate(embedding, metaData);
    }
}
