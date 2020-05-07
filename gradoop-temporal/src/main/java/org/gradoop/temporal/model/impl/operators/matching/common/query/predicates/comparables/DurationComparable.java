package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;

import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.util.ComparableFactory;
import org.s1ck.gdl.model.comparables.time.Duration;

import java.util.HashSet;
import java.util.Set;

/**
 * Wraps a {@link org.s1ck.gdl.model.comparables.time.Duration}
 */
public class DurationComparable extends TemporalComparable {

    /**
     * The wrapped duration
     */
    Duration duration;

    /**
     * the from value of the interval
     */
    TemporalComparable from;

    /**
     * the to value of the interval
     */
    TemporalComparable to;

    /**
     * Creates a new wrapper
     * @param duration the Duration to be wrapped
     */
    public DurationComparable(Duration duration){
        this.duration = duration;
        this.from = (TemporalComparable) ComparableFactory.createComparableFrom(duration.getFrom());
        this.to = (TemporalComparable) ComparableFactory.createComparableFrom(duration.getTo());
    }

    @Override
    public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData) {
        return PropertyValue.create(to.evaluate(embedding, metaData).getLong() -
                from.evaluate(embedding, metaData).getLong());
    }

    @Override
    public PropertyValue evaluate(GraphElement element) {
        return PropertyValue.create(to.evaluate(element).getLong() -
                from.evaluate(element).getLong());
    }

    @Override
    public Set<String> getPropertyKeys(String variable) {
        return new HashSet<>();
    }

    @Override
    public boolean isGlobal() {
        return from.isGlobal() || to.isGlobal();
    }

    @Override
    public boolean equals(Object o){
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DurationComparable that = (DurationComparable) o;
        return (that.from.equals(from) && that.to.equals(to));
    }
}
