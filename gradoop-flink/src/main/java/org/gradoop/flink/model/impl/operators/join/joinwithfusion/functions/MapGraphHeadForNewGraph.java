package org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Created by vasistas on 17/02/17.
 */
public class MapGraphHeadForNewGraph implements MapFunction<GraphHead, GraphHead> {

  private final GradoopId graphId;

  public MapGraphHeadForNewGraph(GradoopId newGraphid) {
    graphId = newGraphid;
  }

  @Override
  public GraphHead map(GraphHead value) throws Exception {
    value.setId(graphId);
    return value;
  }
}
