
package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Superclass of single graph containment filters using broadcast variables.
 *
 * @param <GE> graph element type
 */
public abstract class GraphContainmentFilterBroadcast
  <GE extends GraphElement> extends RichFilterFunction<GE> {

  /**
   * constant string for "graph id"
   */
  public static final String GRAPH_ID = "graphId";

  /**
   * graph id
   */
  protected GradoopId graphId;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    graphId = getRuntimeContext()
      .<GradoopId>getBroadcastVariable(GRAPH_ID).get(0);
  }
}
