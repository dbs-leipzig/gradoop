package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions;

import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TemporalComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.util.ComparableFactory;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

/**
 * Wraps a {@link org.s1ck.gdl.model.predicates.expressions.Comparison}
 * Extended for temporal support
 */
public class ComparisonExpressionTPGM extends ComparisonExpression {

    /**
     * Holds the wrapped comparison
     */
    private final Comparison comparison;

    /**
     * Creates a wrapper from a GDL Comparison
     * @param comparison the GDL Comparison to wrap
     */
    public ComparisonExpressionTPGM(Comparison comparison) {
        super(comparison);
        this.comparison = comparison;
    }

    @Override
    public QueryComparable getLhs() {
        return ComparableFactory.createComparableFrom(comparison.getComparableExpressions()[0]);
    }

    @Override
    public QueryComparable getRhs() {
        return ComparableFactory.createComparableFrom(comparison.getComparableExpressions()[1]);
    }

    


    public Comparison getGDLComparison(){
        return comparison;
    }

    public boolean isTemporal(){
        return comparison.isTemporal();
    }

}
