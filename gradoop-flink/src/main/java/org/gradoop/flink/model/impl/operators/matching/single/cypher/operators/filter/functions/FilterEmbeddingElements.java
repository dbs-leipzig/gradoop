package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

import java.util.ArrayList;
import java.util.List;

/**
 * Filters {@link Embedding} columns based on a given set of variables, which are
 * mapped in a {@link EmbeddingMetaData} object.
 */
public class FilterEmbeddingElements extends RichMapFunction<Embedding, Embedding> {
    /**
     * Return pattern variables that already exist in pattern matching query
     */
    private final List<String> existingReturnPatternVariables;
    /**
     * Meta data for pattern matching embeddings
     */
    private final EmbeddingMetaData embeddingMetaData;
    /**
     * Meta data for return pattern embeddings
     */
    private final EmbeddingMetaData newMetaData;

    /**
     * New embeddings filter function
     *
     * @param existingReturnPatternVariables Join of query and return pattern variables
     * @param embeddingMetaData Pattern matching meta data
     * @param newMetaData Return pattern meta data
     */
    public FilterEmbeddingElements(List<String> existingReturnPatternVariables, EmbeddingMetaData embeddingMetaData,
                                   EmbeddingMetaData newMetaData) {
        this.existingReturnPatternVariables = existingReturnPatternVariables;
        this.embeddingMetaData = embeddingMetaData;
        this.newMetaData = newMetaData;
    }

    @Override
    public Embedding map(Embedding embedding) throws Exception {
        Embedding newEmbedding = new Embedding();
        List<GradoopId> orderedIdList = new ArrayList<>(existingReturnPatternVariables.size());
        for(String existingReturnPatternVariable : existingReturnPatternVariables) {
            orderedIdList.add(
                    newMetaData.getEntryColumn(existingReturnPatternVariable),
                    embedding.getId(embeddingMetaData.getEntryColumn(existingReturnPatternVariable)));
        }

        for(int i=0; i<orderedIdList.size(); i++) {
            newEmbedding.add(orderedIdList.get(i));
        }
        return newEmbedding;
    }
}
