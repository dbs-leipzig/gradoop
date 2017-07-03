package org.gradoop.flink.model.impl.operators.tostring.api;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.gradoop.flink.model.impl.operators.tostring.tuples.EdgeString;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * string representation of an edge
 * @param <E> edge type
 */
public interface EdgeToString<E extends Edge>
  extends FlatMapFunction<E, EdgeString> {
}
