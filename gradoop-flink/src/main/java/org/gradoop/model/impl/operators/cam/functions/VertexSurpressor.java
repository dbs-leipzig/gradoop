package org.gradoop.model.impl.operators.cam.functions;

import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.operators.cam.tuples.VertexLabel;

public class VertexSurpressor<V extends EPGMVertex>
  implements VertexLabeler<V> {

  @Override
  public void flatMap(V v, Collector<VertexLabel> collector) throws Exception {
  }
}
