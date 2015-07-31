package org.gradoop.model.impl.operators;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.impl.EPFlinkVertexData;

import java.io.Serializable;

/**
 * Simple Function defining a mapping from vertex to integer
 */
public interface LongFromVertexFunction extends Serializable, Function {
  Long extractLong(Vertex<Long, EPFlinkVertexData> vertex);
}
