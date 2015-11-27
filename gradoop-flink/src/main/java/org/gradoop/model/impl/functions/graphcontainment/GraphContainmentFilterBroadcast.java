package org.gradoop.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;

public abstract class GraphContainmentFilterBroadcast
  <GE extends EPGMGraphElement> extends RichFilterFunction<GE> {

  public static final String GRAPH_ID = "graphId";
  protected GradoopId graphId;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    graphId = getRuntimeContext()
      .<GradoopId>getBroadcastVariable(GRAPH_ID).get(0);
  }
}
