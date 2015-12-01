package org.gradoop.model.impl.functions.bool;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;

/**
 * Logical and as Flink function
 */
public class And implements CrossFunction<Boolean, Boolean, Boolean>,
  ReduceFunction<Boolean> {

  @Override
  public Boolean cross(Boolean a, Boolean b) throws Exception {
    return a && b;
  }

  @Override
  public Boolean reduce(Boolean a, Boolean b) throws Exception {
    return a && b;
  }

  public static DataSet<Boolean> union(DataSet<Boolean> a, DataSet<Boolean> b) {
    return a.union(b).reduce(new And());
  }

  public static DataSet<Boolean> cross(DataSet<Boolean> a, DataSet<Boolean> b) {
    return a.cross(b).with(new And());
  }
}
