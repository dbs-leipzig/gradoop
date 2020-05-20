package org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo.TemporalElementStats;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

/**
 * Reduces a set of {@link TemporalElement} to a {@link TemporalElementStats} about this set.
 * It is assumed that all elements have the same label.
 */
public class ElementsToStats<T extends TemporalElement> implements
        GroupReduceFunction<T, TemporalElementStats> {

    @Override
    public void reduce(Iterable<T> values, Collector<TemporalElementStats> out) throws Exception {
        TemporalElementStats stats = new TemporalElementStats();
        for(TemporalElement element: values){
            stats.addElement(element);
            stats.setLabel(element.getLabel());
        }
        out.collect(stats);
    }
}
