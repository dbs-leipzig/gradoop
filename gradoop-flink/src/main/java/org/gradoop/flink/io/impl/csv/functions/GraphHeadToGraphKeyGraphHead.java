package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Maps the graph head to a tuple of graph key and graphhead. The key is taken from the heads key
 * property.
 */
public class GraphHeadToGraphKeyGraphHead implements
  MapFunction<GraphHead, Tuple2<String, GraphHead>> {

  @Override
  public Tuple2<String, GraphHead> map(GraphHead graphHead) throws Exception {
    return new Tuple2<String, GraphHead>(graphHead.getPropertyValue("key").getString(), graphHead);
  }
}
