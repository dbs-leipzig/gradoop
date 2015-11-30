package org.gradoop.model.impl;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class GradoopFlinkTestUtils {

  public static <T> T writeAndRead(T element) throws Exception {
    DataSet<T> dataSet = ExecutionEnvironment.getExecutionEnvironment()
      .fromElements(element);
    return dataSet.collect().get(0);
  }
}
