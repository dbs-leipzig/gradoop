
package org.gradoop.flink.model.impl.functions.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * t => (t, count)
 *
 * @param <T> data type of t
 */
public class AddCount<T> implements MapFunction<T, WithCount<T>> {

  @Override
  public WithCount<T> map(T object) throws Exception {
    return new WithCount<>(object);
  }
}
