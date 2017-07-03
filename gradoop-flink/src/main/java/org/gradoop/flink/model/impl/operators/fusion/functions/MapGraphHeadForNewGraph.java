
package org.gradoop.flink.model.impl.operators.fusion.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Creates a new head and sets a new graph id
 */
public class MapGraphHeadForNewGraph implements MapFunction<GraphHead, GraphHead> {

  /**
   * Id to be setted
   */
  private final GradoopId graphId;

  /**
   * Default constructor
   * @param newGraphid   id to be setted
   */
  public MapGraphHeadForNewGraph(GradoopId newGraphid) {
    graphId = newGraphid;
  }

  @Override
  public GraphHead map(GraphHead value) throws Exception {
    value.setId(graphId);
    return value;
  }
}
