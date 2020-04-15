package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

import java.util.List;

/**
 * Projects an Embedding by a set of properties.
 * For each entry in the embedding a different property set can be specified
 */
public class ProjectTemporalEmbedding extends RichMapFunction<EmbeddingTPGM, EmbeddingTPGM> {
    /**
     * Indices of the properties that will be kept in the projection
     */
    private final List<Integer> propertyWhiteList;

    /**
     * Creates a new embedding projection operator
     * @param propertyWhiteList includes all property indexes that whill be kept in the projection
     */
    public ProjectTemporalEmbedding(List<Integer> propertyWhiteList) {
        this.propertyWhiteList = propertyWhiteList;
    }

    @Override
    public EmbeddingTPGM map(EmbeddingTPGM embedding) {
        return embedding.project(propertyWhiteList);
    }
}
