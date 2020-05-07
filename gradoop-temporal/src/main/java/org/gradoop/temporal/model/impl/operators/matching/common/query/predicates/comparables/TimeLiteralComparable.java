package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;

import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;

import java.util.HashSet;
import java.util.Set;

/**
 * Wraps a {@link org.s1ck.gdl.model.comparables.time.TimeLiteral}
 */
public class TimeLiteralComparable extends TemporalComparable {

    /**
     * The wrapped TimeLiteral
     */
    private final TimeLiteral timeLiteral;

    /**
     * Creates a new wrapper
     *
     * @param timeLiteral the wrapped literal
     */
    public TimeLiteralComparable(TimeLiteral timeLiteral){
        this.timeLiteral = timeLiteral;
    }

    @Override
    public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData){
        return PropertyValue.create((long)timeLiteral.evaluate().get());
    }

    @Override
    public PropertyValue evaluate(GraphElement element){
        return PropertyValue.create(
                (long)timeLiteral.evaluate().get());
    }

    @Override
    public Set<String> getPropertyKeys(String variable){
        return new HashSet<>(0);
    }

    @Override
    public boolean equals(Object o){
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimeLiteralComparable that = (TimeLiteralComparable) o;

        return that.timeLiteral.equals(timeLiteral);
    }

    @Override
    public int hashCode(){
        return timeLiteral != null ? timeLiteral.hashCode() : 0;
    }

    @Override
    public boolean isGlobal() {
        return false;
    }
}
