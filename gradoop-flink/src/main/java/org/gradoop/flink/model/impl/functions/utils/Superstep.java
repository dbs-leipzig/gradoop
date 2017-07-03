
package org.gradoop.flink.model.impl.functions.utils;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Returns the current superstep using {@link IterationRuntimeContext}.
 *
 * Note that this function can only be applied in an iterative context (i.e.
 * bulk or delta iteration).
 *
 * @param <T> input type
 */
public class Superstep<T> extends RichMapFunction<T, Integer> {

  /**
   * super step
   */
  private Integer superstep;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    superstep = getIterationRuntimeContext().getSuperstepNumber();
  }

  @Override
  public Integer map(T value) throws Exception {
    return superstep;
  }
}
