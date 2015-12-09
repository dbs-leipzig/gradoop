package org.gradoop.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.List;

/**
 * Adds new graph id's to the edge if source and target vertex are part of
 * the same graph. Filters all edges between graphs.
 *
 * @param <E> EPGM edge Type
 */
public class AddNewGraphsToEdgeFlatMapper<E extends EPGMEdge> implements
  FlatMapFunction<Tuple3<E, List<GradoopId>, List<GradoopId>>, E> {
  @Override
  public void flatMap(
    Tuple3<E, List<GradoopId>, List<GradoopId>> tuple3,
    Collector<E> collector) {
    List<GradoopId> sourceGraphs = tuple3.f1;
    List<GradoopId> targetGraphs = tuple3.f2;
    List<GradoopId> graphsToBeAdded = new ArrayList<>();
    boolean filter = false;
    for (GradoopId id : sourceGraphs) {
      if (targetGraphs.contains(id)) {
        graphsToBeAdded.add(id);
        filter = true;
      }
    }
    E edge = tuple3.f0;
    edge.getGraphIds().addAll(graphsToBeAdded);
    if (filter) {
      collector.collect(edge);
    }
  }
}
