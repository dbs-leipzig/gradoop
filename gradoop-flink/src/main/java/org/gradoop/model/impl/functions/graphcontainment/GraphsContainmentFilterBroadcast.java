package org.gradoop.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Collection;

/**
 * Superclass of multi graph containment filters using broadcast variables.
 *
 * @param <GE> graph element type
 */
public abstract class GraphsContainmentFilterBroadcast
  <GE extends EPGMGraphElement> extends RichFilterFunction<GE> {

  /**
   * constant string for "graph ids"
   */
  public static final String GRAPH_IDS = "graphIds";

  /**
   * graph ids
   */
  protected Collection<GradoopId> graphIds;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    graphIds = getRuntimeContext().getBroadcastVariable(GRAPH_IDS);
  }
}
