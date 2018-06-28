package org.gradoop.flink.model.impl.functions.epgm.filters;


import org.apache.flink.api.common.functions.FilterFunction;

/**
 * A filter that combines multiple filters using a logical {@code or}.
 * This filter will therefore accept all objects that are accepted by
 * any component filter.
 *
 * @param <T> The type of objects to filter.
 */
public class Or<T> extends AbstractRichCombinedFilterFunction<T> {

    /**
     * Constructor setting the filter to be combined with a logical {@code or}.
     *
     * @param filters The filters.
     */
    @SafeVarargs
    public Or(FilterFunction<? super T>... filters) {
        super(filters);
    }

    @Override
    public boolean filter(T value) throws Exception {
        for (FilterFunction<? super T> componentFilter : components) {
            if (componentFilter.filter(value)) {
                return true;
            }
        }
        return false;
    }
}
