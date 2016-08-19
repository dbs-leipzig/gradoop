package org.gradoop.flink.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;

public class GraphIdsTupleFromEdge implements
  MapFunction<Edge, Tuple2<GradoopId, GradoopIdSet>> {

  @Override
  public Tuple2<GradoopId, GradoopIdSet> map(Edge edge) throws Exception {
    return
      new Tuple2<GradoopId, GradoopIdSet>(
        edge.getTargetId(), edge.getGraphIds());

  }
}
