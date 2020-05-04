package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingFactory;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMFactory;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.TripleTPGM;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/**
 * Applies a given predicate on a
 * {@link org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.TripleTPGM} and projects specified property values to the
 * output embedding.
 */
public class FilterAndProjectTemporalTriple extends RichFlatMapFunction<TripleTPGM, EmbeddingTPGM> {
    /**
     * Predicates used for filtering
     */
    private final CNF predicates;
    /**
     * variable of the source vertex
     */
    private final String sourceVariable;
    /**
     * variable of the target vertex
     */
    private final String targetVariable;
    /**
     * Property keys used for value projection of the source vertex
     */
    private final List<String> sourceProjectionPropertyKeys;
    /**
     * Property keys used for value projection of the edge
     */
    private final List<String> edgeProjectionPropertyKeys;
    /**
     * Property keys used for value projection of the target vertex
     */
    private final List<String> targetProjectionPropertyKeys;
    /**
     * Meta data describing the vertex embedding used for filtering
     */
    private final EmbeddingTPGMMetaData filterMetaData;
    /**
     * Source vertex propertyKeys of the embedding used for filtering
     */
    private final List<String> sourceFilterPropertyKeys;
    /**
     * Edge propertyKeys of the embedding used for filtering
     */
    private final List<String> edgeFilterPropertyKeys;
    /**
     * Target vertex propertyKeys of the embedding used for filtering
     */
    private final List<String> targetFilterPropertyKeys;

    /**
     * True if vertex and target variable are the same
     */
    private final boolean isLoop;

    /**
     * Set to true if vertex matching strategy is isomorphism
     */
    private final boolean isVertexIso;

    /**
     * New FilterAndProjectTriples
     * @param sourceVariable the source variable
     * @param edgeVariable edge variabe
     * @param targetVariable target variable
     * @param predicates filter predicates
     * @param projectionPropertyKeys property keys used for projection
     * @param vertexMatchStrategy vertex match strategy
     */
    public FilterAndProjectTemporalTriple(String sourceVariable, String edgeVariable, String targetVariable,
                                  CNF predicates, Map<String, List<String>> projectionPropertyKeys,
                                  MatchStrategy vertexMatchStrategy) {

        this.predicates = predicates;
        this.sourceVariable = sourceVariable;
        this.targetVariable = targetVariable;

        this.sourceProjectionPropertyKeys =
                projectionPropertyKeys.getOrDefault(sourceVariable, new ArrayList<>());
        this.edgeProjectionPropertyKeys =
                projectionPropertyKeys.getOrDefault(edgeVariable, new ArrayList<>());
        this.targetProjectionPropertyKeys =
                projectionPropertyKeys.getOrDefault(targetVariable, new ArrayList<>());

        this.isLoop = sourceVariable.equals(targetVariable);
        this.isVertexIso = vertexMatchStrategy.equals(MatchStrategy.ISOMORPHISM);

        filterMetaData = createFilterMetaData(predicates, sourceVariable, edgeVariable, targetVariable);
        sourceFilterPropertyKeys = filterMetaData.getPropertyKeys(sourceVariable);
        edgeFilterPropertyKeys = filterMetaData.getPropertyKeys(edgeVariable);
        targetFilterPropertyKeys = filterMetaData.getPropertyKeys(targetVariable);

    }

    @Override
    public void flatMap(TripleTPGM triple, Collector<EmbeddingTPGM> out) throws Exception {
        boolean isValid = true;

        if (isLoop) {
            if (!(triple.getSourceId().equals(triple.getTargetId()))) {
                isValid = false;
            }
            // not correct, both variables could have the same name...
        } else if (isVertexIso && triple.getSourceId().equals(triple.getTargetId())) {
            isValid = false;
        }

        if (isValid && filter(triple)) {
            out.collect(
                    EmbeddingTPGMFactory.fromTriple(
                            triple,
                            sourceProjectionPropertyKeys, edgeProjectionPropertyKeys, targetProjectionPropertyKeys,
                            sourceVariable, targetVariable
                    )
            );
        }
    }

    /**
     * Checks if the the triple holds for the predicate
     * @param triple triple to be filtered
     * @return True if the triple holds for the predicate
     */
    private boolean filter(TripleTPGM triple) {
        return predicates.evaluate(
                EmbeddingTPGMFactory.fromTriple(triple,
                        sourceFilterPropertyKeys,  edgeFilterPropertyKeys, targetFilterPropertyKeys,
                        sourceVariable, targetVariable
                ),
                filterMetaData
        );
    }

    /**
     * Creates the {@code EmbeddingTPGMMetaData} of the embedding used for filtering
     * @param predicates filter predicates
     * @param sourceVariable source variable
     * @param edgeVariable edge variable
     * @param targetVariable target variable
     * @return filter embedding meta data
     */
    private static EmbeddingTPGMMetaData createFilterMetaData(CNF predicates, String sourceVariable,
                                                          String edgeVariable, String targetVariable) {

        EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData();
        metaData.setEntryColumn(sourceVariable, EmbeddingTPGMMetaData.EntryType.VERTEX, 0);
        metaData.setEntryColumn(edgeVariable, EmbeddingTPGMMetaData.EntryType.EDGE, 1);
        metaData.setEntryColumn(targetVariable, EmbeddingTPGMMetaData.EntryType.VERTEX, 2);

        metaData.setTimeColumn(sourceVariable, 0);
        metaData.setTimeColumn(edgeVariable, 1);
        metaData.setTimeColumn(targetVariable, 2);

        int i = 0;
        for (String variable : new String[] {sourceVariable, edgeVariable, targetVariable}) {
            for (String propertyKey : predicates.getPropertyKeys(variable)) {
                metaData.setPropertyColumn(variable, propertyKey, i++);
            }
        }


        return metaData;
    }
}
