package org.gradoop.flink.model.impl.functions.epgm.filters;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.util.Objects;

/**
 * A {@link RichFilterFunction} that can be combined like a
 * {@link CombineableFilter}. The {@link RuntimeContext context} will be
 * handled for each filter that is part of this combined filter.
 *
 * @param <T> The type of elements to filter.
 */
public abstract class AbstractRichCombinedFilterFunction<T>
  extends RichFilterFunction<T> implements CombineableFilter<T> {

  /**
   * The filters this filter is composed of.
   */
  protected final FilterFunction<? super T>[] components;

  /**
   * Constructor setting the component filter functions that this filter is
   * composed of.
   *
   * @param filters The filters.
   */
  @SafeVarargs
  public AbstractRichCombinedFilterFunction(
  FilterFunction<? super T>... filters) {
    for (FilterFunction<? super T> filter : filters) {
      Objects.requireNonNull(filter);
    }
    components = filters;
  }

  @Override
  public void setRuntimeContext(RuntimeContext t) {
    super.setRuntimeContext(t);
    for (FilterFunction<? super T> component : components) {
      if (component instanceof RichFunction) {
        ((RichFunction) component).setRuntimeContext(t);
      }
    }
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    for (FilterFunction<? super T> component : components) {
      if (component instanceof RichFunction) {
        ((RichFunction) component).open(parameters);
      }
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    for (FilterFunction<? super T> component : components) {
      if (component instanceof RichFunction) {
        ((RichFunction) component).close();
      }
    }
  }
}
