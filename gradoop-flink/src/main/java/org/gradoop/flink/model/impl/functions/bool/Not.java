
package org.gradoop.flink.model.impl.functions.bool;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;

/**
 * Logical "NOT" as Flink function.
 */
public class Not implements MapFunction<Boolean, Boolean> {

  @Override
  public Boolean map(Boolean b) throws Exception {
    return !b;
  }

  /**
   * Map a a boolean dataset to its inverse.
   *
   * @param b boolean dataset
   * @return inverse dataset
   */
  public static DataSet<Boolean> map(DataSet<Boolean> b) {
    return b.map(new Not());
  }
}
