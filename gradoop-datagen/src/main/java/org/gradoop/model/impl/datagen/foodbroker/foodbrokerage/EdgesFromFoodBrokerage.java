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
public class EdgesFromFoodBrokerage<V extends EPGMVertex, E extends EPGMEdge>
  implements
  FlatMapFunction<Tuple2<Set<V>, Set<E>>, E> {

  @Override
  public void flatMap(
    Tuple2<Set<V>, Set<E>> tuple, Collector<E> collector) throws Exception {
    for (E edge : tuple.f1) {
      collector.collect(edge);
    }
  }
}
