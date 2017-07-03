
package org.gradoop.flink.model.impl.functions.bool;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Logical "TRUE" as Flink function.
 *
 * @param <T> input element type
 */
public class True<T> implements MapFunction<T, Boolean>, FilterFunction<T> {

  @Override
  public Boolean map(T t) throws Exception {
    return true;
  }

  @Override
  public boolean filter(T t) throws Exception {
    return true;
  }
}
