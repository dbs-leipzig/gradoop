package org.gradoop.flink.datagen.foodbroker.complainthandling;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by Stephan on 15.08.16.
 */
public class MergeGraphIds implements
  GroupReduceFunction<Tuple2<String, Vertex>, Vertex> {

  @Override
  public void reduce(Iterable<Tuple2<String, Vertex>> iterable,
    Collector<Vertex> collector) throws Exception {
    Vertex vertex = null;
    GradoopIdSet graphIds = new GradoopIdSet();
    for (Tuple2<String, Vertex> tuple : iterable) {
      graphIds.addAll(tuple.f1.getGraphIds());
      if (vertex == null) {
        vertex = tuple.f1;
      }
    }
    vertex.setGraphIds(graphIds);
    collector.collect(vertex);
  }
}
