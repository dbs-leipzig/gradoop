
package org.gradoop.flink.model.impl.functions.bool;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.DataSet;

/**
 * Equality as Flink function.
 *
 * @param <T> input element type
 */
public class Equals<T>
  implements CrossFunction<T, T, Boolean> {

  @Override
  public Boolean cross(T left, T right) throws Exception {
    return left.equals(right);
  }

  /**
   * Checks for pair-wise equality between the elements of the given input sets.
   *
   * @param first   first input dataset
   * @param second  second input dataset
   * @param <T>     dataset element type
   * @return dataset with {@code boolean} values for each pair
   */
  public static <T> DataSet<Boolean> cross(
    DataSet<T> first, DataSet<T> second) {
    return first.cross(second).with(new Equals<T>());
  }
}
