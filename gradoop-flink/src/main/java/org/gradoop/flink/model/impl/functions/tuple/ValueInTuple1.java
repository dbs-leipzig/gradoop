
package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Wraps a value in a tuple 1.
 * @param <T> value type
 */
public class ValueInTuple1<T> implements MapFunction<T, Tuple1<T>> {

  @Override
  public Tuple1<T> map(T value) throws Exception {
    return new Tuple1<>(value);
  }
}
