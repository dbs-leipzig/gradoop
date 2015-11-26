package org.gradoop.model.impl.functions.filterfunctions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.api.EPGMGraphHead;

/**
 * Created by peet on 26.11.15.
 */
public abstract class AbstractGraphContainmentFilter
  <G extends EPGMGraphHead, GE extends EPGMGraphElement>
  extends RichFilterFunction<GE>{

  public static final String GRAPH_HEAD = "graphHead";
  protected G graphHead;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    graphHead = getRuntimeContext()
      .<G>getBroadcastVariable(GRAPH_HEAD).get(0);
  }
}
