package org.gradoop.model.impl.functions.filterfunctions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.api.EPGMGraphHead;

import java.util.Collection;

/**
 * Created by peet on 26.11.15.
 */
public abstract class AbstractGraphsContainmentFilter
  <G extends EPGMGraphHead, GE extends EPGMGraphElement>
  extends RichFilterFunction<GE>{

  public static final String GRAPH_HEADS = "graphHeads";
  protected Collection<G> graphHeads;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    graphHeads = getRuntimeContext().getBroadcastVariable(GRAPH_HEADS);
  }
}
