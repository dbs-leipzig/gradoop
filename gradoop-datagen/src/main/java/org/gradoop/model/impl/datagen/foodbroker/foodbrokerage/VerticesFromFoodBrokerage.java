package org.gradoop.model.impl.datagen.foodbroker.foodbrokerage;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;

import java.util.Set;

/**
 * Created by Stephan on 01.08.16.
 */
public class VerticesFromFoodBrokerage<V extends EPGMVertex, E extends EPGMEdge>
  implements
  FlatMapFunction<Tuple2<Set<V>, Set<E>>, V> {

  @Override
  public void flatMap(
    Tuple2<Set<V>, Set<E>> tuple, Collector<V> collector) throws Exception {
    for (V vertex : tuple.f0) {
      collector.collect(vertex);
    }
  }
}
