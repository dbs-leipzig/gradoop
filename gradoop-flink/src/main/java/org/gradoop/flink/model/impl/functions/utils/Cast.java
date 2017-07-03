
package org.gradoop.flink.model.impl.functions.utils;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Casts an object of type {@link IN} to type {@link T}.
 *
 * @param <IN>  input type
 * @param <T>   type to cast to
 */
public class Cast<IN, T> implements MapFunction<IN, T> {

  /**
   * Class for type cast
   */
  private final Class<T> clazz;

  /**
   * Constructor
   *
   * @param clazz class for type cast
   */
  public Cast(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public T map(IN value) throws Exception {
    return clazz.cast(value);
  }
}
