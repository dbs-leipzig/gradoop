package org.gradoop.model.helper;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.VertexData;

import java.io.Serializable;

/**
 * Simple Function defining a mapping from vertex to integer
 */
public interface LongFromVertexFunction<VD extends VertexData> extends
  Serializable, Function {
  Long extractLong(Vertex<Long, VD> vertex);
}
