package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;

import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.util.ComparableFactory;
import org.s1ck.gdl.model.comparables.time.Duration;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;

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
     * Long describing the current system time
     */
    // TODO set it from the outside
    Long now;

    /**
     * Creates a new wrapper
     * @param duration the Duration to be wrapped
     */
    public DurationComparable(Duration duration){
        this.duration = duration;
        this.from = (TemporalComparable) ComparableFactory.createComparableFrom(duration.getFrom());
        this.to = (TemporalComparable) ComparableFactory.createComparableFrom(duration.getTo());
        this.now = new TimeLiteral("now").getMilliseconds();
    }

    @Override
    public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData) {
        Long toLong = to.evaluate(embedding, metaData).getLong() == Long.MAX_VALUE ?
                now : to.evaluate(embedding, metaData).getLong();
        return PropertyValue.create(toLong -
                from.evaluate(embedding, metaData).getLong());
    }

    @Override
    public PropertyValue evaluate(GraphElement element) {
        Long toLong = to.evaluate(element).getLong() == Long.MAX_VALUE ?
                now : to.evaluate(element).getLong();
        return PropertyValue.create(toLong -
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
