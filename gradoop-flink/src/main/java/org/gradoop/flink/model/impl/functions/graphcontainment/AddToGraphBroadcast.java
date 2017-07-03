
package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Adds the given graph head identifier to the graph element. The identifier
 * is transferred via broadcasting.
 *
 * @param <GE> EPGM graph element type
 */
@FunctionAnnotation.ForwardedFields("id;label;properties")
public class AddToGraphBroadcast
  <GE extends GraphElement>
  extends RichMapFunction<GE, GE> {

  /**
   * constant string for "graph id"
   */
  public static final String GRAPH_ID = "graphId";

  /**
   * Graph head identifier which gets added to the graph element.
   */
  private GradoopId graphId;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    graphId = getRuntimeContext()
      .<GradoopId>getBroadcastVariable(GRAPH_ID).get(0);
  }

  @Override
  public GE map(GE graphElement) throws Exception {
    graphElement.addGraphId(graphId);
    return graphElement;
  }
}
