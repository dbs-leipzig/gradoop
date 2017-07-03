
package org.gradoop.flink.model.impl.functions.bool;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;

/**
 * Logical "OR" as Flink function.
 */
public class Or implements ReduceFunction<Boolean>,
  CrossFunction<Boolean, Boolean, Boolean> {
  @Override
  public Boolean reduce(Boolean first, Boolean second) throws Exception {
    return first || second;
  }

  @Override
  public Boolean cross(Boolean first, Boolean second) throws Exception {
    return first || second;
  }

  /**
   * Performs a logical disjunction on the union of both input data sets.
   *
   * @param a boolean vector a
   * @param b boolean vector b
   * @return 1-element dataset containing the result of the conjunction
   */
  public static DataSet<Boolean> union(DataSet<Boolean> a, DataSet<Boolean> b) {
    return a.union(b).reduce(new Or());
  }

  /**
   * Performs a pair-wise logical disjunction on the cross of both input data
   * sets.
   *
   * @param a boolean vector a
   * @param b boolean vector b
   * @return dataset containing the result of the pair-wise conjunction
   */
  public static DataSet<Boolean> cross(DataSet<Boolean> a, DataSet<Boolean> b) {
    return a.cross(b).with(new Or());
  }

  /**
   * Performs a logical disjunction on a data set of boolean values.
   *
   * @param d boolean set
   * @return disjunction
   */
  public static DataSet<Boolean> reduce(DataSet<Boolean> d) {
    return d.reduce(new Or());
  }
}
