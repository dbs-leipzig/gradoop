
package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Adds the given graph head identifier to the graph element.
 *
 * @param <GE> EPGM graph element
 */
public class AddToGraph<GE extends GraphElement> implements
  MapFunction<GE, GE> {

  /**
   * Graph head identifier which gets added to the graph element.
   */
  private final GradoopId graphHeadId;

  /**
   * Creates a new GraphContainmentUpdater
   *
   * @param graphHead graph head used for updating
   */
  public AddToGraph(GraphHead graphHead) {
    this.graphHeadId = graphHead.getId();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GE map(GE graphElement) throws Exception {
    graphElement.addGraphId(graphHeadId);
    return graphElement;
  }
}
