package org.gradoop.flink.model.impl.operators.fusion.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Checks whether the edge contains a given graphId, which means that belongs to a given graph
 *
 * Created by Giacomo Bergami on 14/02/17.
 */
public class FilterSubgraphEdges implements FilterFunction<Tuple2<GradoopId, Edge>> {
  @Override
  public boolean filter(Tuple2<GradoopId, Edge> value) throws Exception {
    return value.f1.getGraphIds().contains(value.f0);
  }
}
