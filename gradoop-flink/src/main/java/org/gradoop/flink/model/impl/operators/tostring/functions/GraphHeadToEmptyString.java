package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;

/**
 * represents a graph head by an empty string
 */
public class GraphHeadToEmptyString implements GraphHeadToString<GraphHead> {

  @Override
  public GraphHeadString map(GraphHead graphHead) throws Exception {
    return new GraphHeadString(graphHead.getId(), "");
  }
}
