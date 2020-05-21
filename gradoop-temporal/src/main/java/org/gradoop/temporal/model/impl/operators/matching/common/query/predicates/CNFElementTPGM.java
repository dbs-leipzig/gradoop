package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates;



import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.PredicateCollection;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents a collection disjunct predicates
 * This can be used to represent a CNF
 */
public class CNFElementTPGM extends TemporalPredicateCollection<ComparisonExpressionTPGM> {

    /**
     * Creates a new CNFElement with empty predicate list
     */
    public CNFElementTPGM() {
        this.predicates = new ArrayList<>();
    }

    /**
     * Creats a new CNFElement with preset predicate list
     *
     * @param predicates predicates
     */
    public CNFElementTPGM(List<ComparisonExpressionTPGM> predicates) {
        this.predicates = predicates;
    }

    @Override
    public boolean evaluate(Embedding embedding, EmbeddingMetaData metaData) {
        for (ComparisonExpressionTPGM comparisonExpression : predicates) {
            if (comparisonExpression.evaluate(embedding, metaData)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean evaluate(GraphElement element) {
        for (ComparisonExpressionTPGM comparisonExpression : predicates) {
            if (comparisonExpression.evaluate(element)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<String> getVariables() {
        Set<String> variables = new HashSet<>();
        for (ComparisonExpressionTPGM comparisonExpression : predicates) {
            variables.addAll(comparisonExpression.getVariables());
        }
        return variables;
    }

    @Override
    public Set<String> getPropertyKeys(String variable) {
        Set<String> properties = new HashSet<>();
        for (ComparisonExpressionTPGM comparisonExpression : predicates) {
            properties.addAll(comparisonExpression.getPropertyKeys(variable));
        }
        return properties;
    }

    @Override
    public String operatorName() {
        return "OR";
    }
}

