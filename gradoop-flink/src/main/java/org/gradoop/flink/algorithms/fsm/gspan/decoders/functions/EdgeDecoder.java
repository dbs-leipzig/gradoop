package org.gradoop.flink.algorithms.fsm.gspan.decoders.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Created by peet on 18.08.16.
 */
public class EdgeDecoder implements MapFunction
  <Tuple4<GradoopId, GradoopId, GradoopId, Integer>, Edge> {

  @Override
  public Edge map(Tuple4<GradoopId, GradoopId, GradoopId, Integer> value) throws
    Exception {
    return null;
  }
}
