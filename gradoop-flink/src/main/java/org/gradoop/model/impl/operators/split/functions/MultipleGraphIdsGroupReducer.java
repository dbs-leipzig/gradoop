package org.gradoop.model.impl.operators.split.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.List;

/**
 * groupReduce each group of vertices into a single vertex, whose graphId set
 * contains all graphs of each origin vertex
 */
public class MultipleGraphIdsGroupReducer implements
  GroupReduceFunction
    <Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, List<GradoopId>>> {

  @Override
  public void reduce(
    Iterable<Tuple2<GradoopId, GradoopId>> iterable,
    Collector<Tuple2<GradoopId, List<GradoopId>>> collector) {
    List<Tuple2<GradoopId, GradoopId>> tuples =
      Lists.newArrayList(iterable);
    List<GradoopId> newGraphs = new ArrayList<>();
    Tuple2<GradoopId, GradoopId> firstTuple = tuples.get(0);
    for (Tuple2<GradoopId, GradoopId> tuple: tuples) {
      newGraphs.add(tuple.f1);
    }
    collector.collect(new Tuple2<>(firstTuple.f0, newGraphs));
  }
}
