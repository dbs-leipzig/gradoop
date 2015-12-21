package org.gradoop.model.impl.operators.cam.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.operators.cam.tuples.VertexLabel;

public interface VertexLabeler<V extends EPGMVertex>
  extends FlatMapFunction<V, VertexLabel> {
  @Override
  void flatMap(V v, Collector<VertexLabel> collector) throws Exception;

}
