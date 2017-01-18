package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Maps the graph key to a tuple of graph key and graphhead where the head is null.
 */
public class GraphKeyToGraphKeyNullGraphHead
  implements MapFunction<String, Tuple2<String, GraphHead>> {

  @Override
  public Tuple2<String, GraphHead> map(String s) throws Exception {
    return new Tuple2<String, GraphHead>(s, null);
  }
}

