package org.gradoop.model.impl.functions.utils;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Checks if a given object of type {@link IN} is instance of a specific class
 * of type {@link T}.
 */
public class IsInstance<IN, T> implements FilterFunction<IN> {
  /**
   * Class for isInstance check
   */
  private final Class<T> clazz;

  /**
   * Constructor
   *
   * @param clazz class for isInstance check
   */
  public IsInstance(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public boolean filter(IN value) throws Exception {
    return clazz.isInstance(value);
  }
}
