package org.gradoop.model.impl.datagen.foodbroker.foodbrokerage;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;

import java.util.Set;

/**
 * Created by Stephan on 01.08.16.
 */
public class GraphHeadsFromFoodBrokerage<G extends EPGMGraphHead, V extends
  EPGMVertex, E extends EPGMEdge>
  implements MapFunction<Tuple2<Set<V>, Set<E>>, G> {

  EPGMGraphHeadFactory<G> graphHeadFactory;

  public GraphHeadsFromFoodBrokerage(EPGMGraphHeadFactory graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  @Override
  public G map(Tuple2<Set<V>, Set<E>> tuple) throws Exception {
    return graphHeadFactory.initGraphHead(tuple.f0.iterator().next()
      .getGraphIds().iterator().next());
  }
}
