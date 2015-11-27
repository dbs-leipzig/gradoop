package org.gradoop.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Collection;

public abstract class GraphsContainmentFilterBroadcast
  <GE extends EPGMGraphElement> extends RichFilterFunction<GE>{

  public static final String GRAPH_IDS = "graphIds";
  protected Collection<GradoopId> graphIds;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    graphIds = getRuntimeContext().getBroadcastVariable(GRAPH_IDS);
  }
}
