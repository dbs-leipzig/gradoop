package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;
import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.s1ck.gdl.model.comparables.time.PlusTimePoint;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;

import java.util.HashSet;
import java.util.Set;

/**
 * Wraps a {@link org.s1ck.gdl.model.comparables.time.PlusTimePoint}
 */
public abstract class PlusTimePointComparable extends QueryComparable {

    /**
     * The wrapped PlusTimePoint
     *//*
    PlusTimePoint plusTimePoint;

    *//**
     * Creates a new wrapper
     *
     * @param plusTimePoint the wrapped PlusTimePoint
     *//*
    public PlusTimePointComparable(PlusTimePoint plusTimePoint) {
        this.plusTimePoint = plusTimePoint;
    }

    @Override
    public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData){
        if (plusTimePoint.getTimePoint() instanceof TimeLiteral) {
            return PropertyValue.create(plusTimePoint.evaluate());
        }
        if (plusTimePoint.getTimePoint() instanceof TimeSelector) {
            Long evalSelector =
                    new TimeSelectorComparable((TimeSelector) plusTimePoint.getTimePoint())
                            .evaluate(embedding, metaData).getLong();
            Long add = plusTimePoint.getConstantMillis();
            return PropertyValue.create(evalSelector + add);
        }
        if (plusTimePoint.getTimePoint() instanceof PlusTimePoint) {
            Long evalPlus =
                    new PlusTimePointComparable((PlusTimePoint) plusTimePoint.getTimePoint())
                            .evaluate(embedding, metaData).getLong();
            Long add = plusTimePoint.getConstantMillis();
            return PropertyValue.create(evalPlus + add);
        }
        return null;
    }

    @Override
    public PropertyValue evaluate(GraphElement element) {
        if (plusTimePoint.getTimePoint() instanceof TimeLiteral) {
            return PropertyValue.create(plusTimePoint.evaluate());
        }
        if (plusTimePoint.getTimePoint() instanceof TimeSelector) {
            Long evalSelector =
                    new TimeSelectorComparable((TimeSelector) plusTimePoint.getTimePoint())
                            .evaluate(element).getLong();
            Long add = plusTimePoint.getConstantMillis();
            return PropertyValue.create(evalSelector + add);
        }
        if (plusTimePoint.getTimePoint() instanceof PlusTimePoint) {
            Long evalPlus =
                    new PlusTimePointComparable((PlusTimePoint) plusTimePoint.getTimePoint())
                            .evaluate(element).getLong();
            Long add = plusTimePoint.getConstantMillis();
            return PropertyValue.create(evalPlus + add);
        }
        return null;
    }

    @Override
    public Set<String> getPropertyKeys(String variable){
        return new HashSet<>(0);
    }

    @Override
    public boolean equals(Object o){
        if(this == o){
            return true;
        }
        if (o == null || getClass() != o.getClass()){
            return false;
        }

        PlusTimePointComparable that = (PlusTimePointComparable) o;

        return that.plusTimePoint.equals(plusTimePoint);
    }

    @Override
    public int hashCode(){
        return plusTimePoint != null ? plusTimePoint.hashCode() : 0;
    }
*/
}
