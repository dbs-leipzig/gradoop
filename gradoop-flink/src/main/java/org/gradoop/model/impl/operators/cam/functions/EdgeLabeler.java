package org.gradoop.model.impl.operators.cam.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.operators.cam.tuples.EdgeLabel;

public interface EdgeLabeler<E extends EPGMEdge>
  extends FlatMapFunction<E, EdgeLabel> {
}
