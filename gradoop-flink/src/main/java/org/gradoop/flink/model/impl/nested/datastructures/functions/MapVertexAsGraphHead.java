package org.gradoop.flink.model.impl.nested.datastructures.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Maps a vertex back to a GraphHead
 */
public class MapVertexAsGraphHead implements MapFunction<Vertex, GraphHead>,
  FlatJoinFunction<Vertex, GraphHead, GraphHead> {

  /**
   * Reusable element
   */
  private final GraphHead reusable;

  /**
   * Default constructor
   */
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
    if (second == null) {
      out.collect(map(first));
    }
  }
}
