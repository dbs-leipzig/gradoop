package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpandEmbeddingTPGM;

/**
 * Filters results from previous iterations
 */
public class FilterPreviousExpandEmbeddingTPGM extends RichFilterFunction<ExpandEmbeddingTPGM> {
    /**
     * super step
     */
    private int currentIteration;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        currentIteration = getIterationRuntimeContext().getSuperstepNumber() * 2 - 1;
    }

    @Override
    public boolean filter(ExpandEmbeddingTPGM expandEmbedding) {
        return expandEmbedding.pathSize() >= currentIteration;
    }
}
