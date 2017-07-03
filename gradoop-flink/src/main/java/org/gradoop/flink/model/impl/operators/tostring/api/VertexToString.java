package org.gradoop.flink.model.impl.operators.tostring.api;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.tostring.tuples.VertexString;

/**
 * string representation of a vertex
 * @param <V> graph head type
 */
public interface VertexToString<V extends Vertex>
  extends FlatMapFunction<V, VertexString> {
}
