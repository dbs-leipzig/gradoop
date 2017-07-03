package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.gradoop.flink.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * represents a graph head by a data string (label and properties)
 */
public class GraphHeadToDataString extends ElementToDataString<GraphHead>
  implements GraphHeadToString<GraphHead> {

  @Override
  public GraphHeadString map(GraphHead graph) throws Exception {
    return new GraphHeadString(graph.getId(), "|" + label(graph) + "|");
  }
}
