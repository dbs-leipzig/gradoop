package org.gradoop.model.helper;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.impl.EPFlinkVertexData;

import java.io.Serializable;
import java.util.Set;

/**
 * Simple Function defining a mapping from vertex to integer
 */
public interface LongSetFromVertexFunction extends Serializable, Function {
  Set<Long> extractLongSet(Vertex<Long, EPFlinkVertexData> vertex);
}