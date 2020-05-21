package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;

import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.QueryComparableTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.util.ComparableFactory;
import org.s1ck.gdl.model.comparables.time.MaxTimePoint;
import org.s1ck.gdl.model.comparables.time.TimePoint;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Wraps a {@link org.s1ck.gdl.model.comparables.time.MaxTimePoint}
 */
public class MaxTimePointComparable extends TemporalComparable {

    /**
     * The wrapped MaxTimePoint
     */
    MaxTimePoint maxTimePoint;

    /**
     * Wrappers for the arguments
     */
    ArrayList<QueryComparableTPGM> args;

    /**
     * Creates a new wrapper.
     *
     * @param maxTimePoint the wrapped MaxTimePoint.
     */
    public MaxTimePointComparable(MaxTimePoint maxTimePoint){
        this.maxTimePoint = maxTimePoint;
        args = new ArrayList<>();
        for(TimePoint arg: maxTimePoint.getArgs()){
            args.add(ComparableFactory.createComparableFrom(arg));
        }
    }

    @Override
    public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData) {
        long max = Long.MIN_VALUE;
        for(QueryComparableTPGM arg: args){
            long argValue = arg.evaluate(embedding, metaData).getLong();
            if(argValue > max){
                max = argValue;
            }
        }
        return PropertyValue.create(max);
    }

    // not implemented, as it is never needed
    @Override
    public PropertyValue evaluate(GraphElement element) {
        if(maxTimePoint.getVariables().size()>1){
            throw new UnsupportedOperationException("can not evaluate an expression with >1 variable on" +
                    " a single GraphElement!");
        }
        long max = Long.MIN_VALUE;
        for(QueryComparableTPGM arg: args){
            long argValue = arg.evaluate(element).getLong();
            if(argValue > max){
                max = argValue;
            }
        }
        return PropertyValue.create(max);
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

        MaxTimePointComparable that = (MaxTimePointComparable) o;
        if(that.args.size() != args.size()){
            return false;
        }

        for(QueryComparableTPGM arg: args){
            boolean foundMatch = false;
            for(QueryComparableTPGM candidate: that.args){
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
        return maxTimePoint != null ? maxTimePoint.hashCode() : 0;
    }

    @Override
    public boolean isGlobal() {
        return maxTimePoint.isGlobal();
    }

    @Override
    public TimePoint getWrappedComparable() {
        return maxTimePoint;
    }
}

