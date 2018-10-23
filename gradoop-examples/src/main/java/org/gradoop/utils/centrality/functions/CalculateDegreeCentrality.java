package org.gradoop.utils.centrality.functions;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple1;

public class CalculateDegreeCentrality implements CrossFunction<Tuple1<Long>, Long, Double> {


  @Override
  public Double cross(Tuple1<Long> val1, Long vertexCount) throws Exception {
    long sum = val1.f0;
    return (double) sum / ((vertexCount - 2)*(vertexCount - 1));
  }
}
