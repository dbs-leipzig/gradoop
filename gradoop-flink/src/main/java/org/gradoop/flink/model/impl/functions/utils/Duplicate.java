
package org.gradoop.flink.model.impl.functions.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Duplicates a data set element k times.
 *
 * @param <T> element type
 */
public class Duplicate<T> implements FlatMapFunction<T, T> {

  /**
   * number of duplicates per input element (k)
   */
  private final int multiplicand;

  /**
   * Constructor.
   *
   * @param multiplicand number of duplicates per input element
   */
  public Duplicate(int multiplicand) {
    this.multiplicand = multiplicand;
  }

  @Override
  public void flatMap(T original, Collector<T> duplicates) throws  Exception {
    for (int i = 1; i <= multiplicand; i++) {
      duplicates.collect(original);
    }
  }
}
