package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.List;

/**
 * Applies a given predicate on a {@link org.gradoop.temporal.model.impl.pojo.TemporalVertex}
 * and projects specified property values to the output embedding.
 */
public class FilterAndProjectTemporalVertex extends
        RichFlatMapFunction<TemporalVertex, EmbeddingTPGM> {
    /**
     * Predicates used for filtering
     */
    private final CNF predicates;
    /**
     * Property keys used for value projection
     */
    private final List<String> projectionPropertyKeys;

    /**
     * New vertex filter function
     *
     * @param predicates predicates used for filtering
     * @param projectionPropertyKeys property keys that will be used for projection
     */
    public FilterAndProjectTemporalVertex(CNF predicates, List<String> projectionPropertyKeys) {
        System.out.println("FilterAndProjectTemporalVertex predicates: "+predicates);
        this.predicates = predicates;
        this.projectionPropertyKeys = projectionPropertyKeys;
    }

    @Override
    public void flatMap(TemporalVertex vertex, Collector<EmbeddingTPGM> out) throws Exception {
        if (predicates.evaluate(vertex)) {
            out.collect(EmbeddingTPGMFactory.fromVertex(vertex, projectionPropertyKeys));
        }
    }

}
