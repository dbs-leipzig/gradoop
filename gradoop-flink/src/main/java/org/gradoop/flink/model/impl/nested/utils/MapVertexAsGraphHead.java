package org.gradoop.flink.model.impl.nested.utils;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by vasistas on 09/03/17.
 */
public class MapVertexAsGraphHead implements MapFunction<Vertex, GraphHead>,
  FlatJoinFunction<Vertex,GraphHead,GraphHead> {

  private final GraphHead reusable;

  public MapVertexAsGraphHead() {
    reusable = new GraphHead();
  }

  @Override
  public GraphHead map(Vertex value) throws Exception {
    reusable.setId(value.getId());
    reusable.setLabel(value.getLabel());
    reusable.setProperties(value.getProperties());
    return reusable;
  }

  @Override
  public void join(Vertex first, GraphHead second, Collector<GraphHead> out) throws Exception {
    if (second==null) {
      out.collect(map(first));
    }
  }
}
