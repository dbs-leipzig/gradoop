package org.gradoop.flink.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.util.Set;

public class UserClients
  implements FlatMapFunction<Tuple2<GraphTransaction, Set<Vertex>>, Vertex> {

  @Override
  public void flatMap(Tuple2<GraphTransaction, Set<Vertex>> tuple,
    Collector<Vertex> collector) throws Exception {
    for (Vertex vertex : tuple.f1) {
      collector.collect(vertex);
    }
  }
}
