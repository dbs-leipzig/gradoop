
package org.gradoop.flink.model.api.functions;

import java.io.Serializable;

/**
 * A serializable function with single input and output.
 *
 * @param <T> the type of the input to the function
 * @param <R> the type of the result of the function
 */
public interface Function<T, R> extends Serializable {
  /**
   * Creates output from given input.
   *
   * @param entity some entity
   * @return some object
   */
  R apply(T entity);
}
