package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;

import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.util.ComparableFactory;
import org.s1ck.gdl.model.comparables.time.MinTimePoint;
import org.s1ck.gdl.model.comparables.time.TimePoint;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Wraps a {@link org.s1ck.gdl.model.comparables.time.MinTimePoint}
 */
public class MinTimePointComparable extends TemporalComparable {

    /**
     * The wrapped MinTimePoint
     */
    MinTimePoint minTimePoint;

    /**
     * Wrappers for the arguments
     */
    ArrayList<QueryComparable> args;

    /**
     * Creates a new wrapper.
     *
     * @param minTimePoint the wrapped MinTimePoint.
     */
    public MinTimePointComparable(MinTimePoint minTimePoint){
        this.minTimePoint = minTimePoint;
        args = new ArrayList<>();
        for(TimePoint arg: minTimePoint.getArgs()){
            args.add(ComparableFactory.createComparableFrom(arg));
        }
    }

    @Override
    public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData) {
        long min = Long.MAX_VALUE;
        for(QueryComparable arg: args){
            long argValue = arg.evaluate(embedding, metaData).getLong();
            if(argValue < min){
                min = argValue;
            }
        }
        return PropertyValue.create(min);
    }

    @Override
    public PropertyValue evaluate(GraphElement element) {
        if(minTimePoint.getVariables().size()>1){
            throw new UnsupportedOperationException("can not evaluate an expression with >1 variable on" +
                    " a single GraphElement!");
        }
        long min = Long.MAX_VALUE;
        for(QueryComparable arg: args){
            long argValue = arg.evaluate(element).getLong();
            if(argValue < min){
                min = argValue;
            }
        }
        return PropertyValue.create(min);
    }

    @Override
    public Set<String> getPropertyKeys(String variable) {
        return new HashSet<>();
    }

    @Override
    public boolean equals(Object o){
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MinTimePointComparable that = (MinTimePointComparable) o;
        if(that.args.size() != args.size()){
            return false;
        }

        for(QueryComparable arg: args){
            boolean foundMatch = false;
            for(QueryComparable candidate: that.args){
                if(arg.equals(candidate)){
                    foundMatch= true;
                }
            }
            if(!foundMatch){
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode(){
        return minTimePoint != null ? minTimePoint.hashCode() : 0;
    }

    @Override
    public boolean isGlobal() {
        return minTimePoint.isGlobal();
    }
}
