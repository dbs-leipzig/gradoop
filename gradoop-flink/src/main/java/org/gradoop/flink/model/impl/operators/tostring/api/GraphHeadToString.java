package org.gradoop.flink.model.impl.operators.tostring.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;

/**
 * string representation of a graph head
 * @param <G> graph head type
 */
public interface GraphHeadToString<G extends GraphHead>
  extends MapFunction<G, GraphHeadString> {
}
