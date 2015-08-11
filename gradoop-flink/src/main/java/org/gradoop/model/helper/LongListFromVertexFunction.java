package org.gradoop.model.helper;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.impl.EPFlinkVertexData;

import java.io.Serializable;
import java.util.List;

/**
 * Simple Function defining a mapping from vertex to integer
 */
public interface LongListFromVertexFunction extends Serializable, Function {
  /**
   * Simple Function to extract a List out of a EPVertex
   *
   * @param vertex EPVertex
   * @return List of Long values
   */
  List<Long> extractLongList(Vertex<Long, EPFlinkVertexData> vertex);
}