package org.gradoop.flink.model.impl.functions.epgm.filters;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * A filter that inverts a filter by using a logical {@code not}.
 * This filter will therefore accept all objects that are not accepted by
 * the original filter.
 *
 * @param <T> The type of objects to filter.
 */
public class Not<T> extends AbstractRichCombinedFilterFunction<T> {

    /**
     * Constructor setting the filter to invert using a logical {@code not}.
     *
     * @param filter The filter.
     */
    public Not(FilterFunction<? super T> filter) {
        super(filter);
    }

    @Override
    public boolean filter(T value) throws Exception {
        return !components[0].filter(value);
    }
}
