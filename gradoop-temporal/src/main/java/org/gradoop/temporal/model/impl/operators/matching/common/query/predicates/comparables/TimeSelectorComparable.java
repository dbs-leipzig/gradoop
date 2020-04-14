package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;
import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphElement;
import org.s1ck.gdl.model.comparables.time.TimeSelector;

import java.util.HashSet;
import java.util.Set;

/**
 * Wraps an {@link org.s1ck.gdl.model.comparables.time.TimeSelector}
 */
public class TimeSelectorComparable extends QueryComparable {

    /**
     * The wrapped TimeSelector
     */
    private final TimeSelector timeSelector;

    /**
     * Creates a new wrapper
     *
     * @param timeSelector the wrapped literal
     */
    public TimeSelectorComparable(TimeSelector timeSelector){
        this.timeSelector = timeSelector;
    }

    /**
     * Returns the variable of the wrapped TimeSelector
     *
     * @return variable of the TimeSelector
     */
    public String getVariable(){
        return timeSelector.getVariable();
    }

    /**
     * Returns the TimeField (i.e. TX_FROM, TX_TO, VAL_FROM or VAL_TO) of the wrapped TimeSelector
     *
     * @return TimeField of wrapped TimeSelector
     */
    public TimeSelector.TimeField getTimeField(){
        return timeSelector.getTimeProp();
    }

    @Override
    public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData){
        //cast into TPGM subclasses
        int timeColumn = ((EmbeddingTPGMMetaData) metaData).getTimeColumn(timeSelector.getVariable());
        Long[] timeValues = ((EmbeddingTPGM) embedding).getTimes(timeColumn);

        TimeSelector.TimeField field = timeSelector.getTimeProp();

        Long time = -1L;
        if(field.equals(TimeSelector.TimeField.TX_FROM)) {
            time = timeValues[0];
        }
        else if(field == TimeSelector.TimeField.TX_TO){
            time = timeValues[1];
        }
        else if(field == TimeSelector.TimeField.VAL_FROM){
            time = timeValues[2];
        }
        else if(field == TimeSelector.TimeField.VAL_TO){
            time = timeValues[3];
        }

        return PropertyValue.create(time);
    }

    @Override
    public PropertyValue evaluate(GraphElement element){
        Long time = -1L;
        TimeSelector.TimeField field = timeSelector.getTimeProp();
        if(field.equals(TimeSelector.TimeField.TX_FROM)) {
            time = ((TemporalGraphElement) element).getTxFrom();
        }
        else if(field == TimeSelector.TimeField.TX_TO){
            time = ((TemporalGraphElement) element).getTxTo();
        }
        else if(field == TimeSelector.TimeField.VAL_FROM){
            time = ((TemporalGraphElement) element).getValidFrom();
        }
        else if(field == TimeSelector.TimeField.VAL_TO){
            time = ((TemporalGraphElement) element).getValidTo();
        }
        return PropertyValue.create(time);
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

        TimeSelectorComparable that = (TimeSelectorComparable) o;

        return that.timeSelector.equals(timeSelector);
    }

    @Override
    public int hashCode(){
        return timeSelector != null ? timeSelector.hashCode() : 0;
    }
}
