package org.gradoop.flink.model.impl.functions.epgm.filters;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * A filter function that can be combined using a logical {@code and},
 * {@code or} and {@code not}.
 *
 * @param <T> The type of elements to filter.
 */
public interface CombineableFilter<T> extends FilterFunction<T> {

    /**
     * Combine this filter with another filter using a logical {@code and}.
     * Either filter may be a
     * {@link org.apache.flink.api.common.functions.RichFunction}.
     *
     * @param other The filter that will be combined with this filter.
     * @return The combined filter.
     */
    default CombineableFilter<T> and(FilterFunction<? super T> other) {
        return new And<>(this, other);
    }

    /**
     * Combine this filter with another filter using a logical {@code or}.
     * Either filter may be a
     * {@link org.apache.flink.api.common.functions.RichFunction}.
     *
     * @param other The filter that will be combined with this filter.
     * @return The combined filter.
     */
    default CombineableFilter<T> or(FilterFunction<? super T> other) {
        return new Or<>(this, other);
    }

    /**
     * Invert this filter, i.e. get the logical {@code not} of this filter.
     * This filter may be a
     * {@link org.apache.flink.api.common.functions.RichFunction}.
     *
     * @return The inverted filter.
     */
    default CombineableFilter<T> negate() {
        return new Not<>(this);
    }
}
