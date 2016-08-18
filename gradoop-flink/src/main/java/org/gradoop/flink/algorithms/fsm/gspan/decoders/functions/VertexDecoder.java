package org.gradoop.flink.algorithms.fsm.gspan.decoders.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;

/**
 * Created by peet on 18.08.16.
 */
public class VertexDecoder
  implements  MapFunction<Tuple3<GradoopId, GradoopId, Integer>, Vertex> {
  public VertexDecoder(VertexFactory vertexFactory) {
  }

  @Override
  public Vertex map(Tuple3<GradoopId, GradoopId, Integer> value) throws
    Exception {
    return null;
  }
}
