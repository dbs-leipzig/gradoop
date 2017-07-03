
package org.gradoop.flink.model.impl.functions.bool;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Logical false as Flink function.
 *
 * @param <T> data set type
 */
public class False<T> implements FilterFunction<T> {

  @Override
  public boolean filter(T t) throws Exception {
    return false;
  }
}
