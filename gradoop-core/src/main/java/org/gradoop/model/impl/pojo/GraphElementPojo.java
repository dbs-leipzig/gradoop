package org.gradoop.model.impl.pojo;

import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.api.EPGMProperties;

/**
 * Abstract class representing an EPGM element that is contained in a logical
 * graph (i.e. vertices and edges).
 */
public abstract class GraphElementPojo
  extends ElementPojo
  implements EPGMGraphElement {

  /**
   * Set of graph identifiers that element is contained in
   */
  private GradoopIdSet graphIds;

  /**
   * Default constructor.
   */
  protected GraphElementPojo() {
  }

  /**
   * Creates an EPGM graph element using the given arguments.
   *  @param id         element id
   * @param label      element label
   * @param properties element properties
   * @param graphIds     graphIds that element is contained in
   */
  protected GraphElementPojo(GradoopId id, String label,
    EPGMProperties properties, GradoopIdSet graphIds) {
    super(id, label, properties);
    this.graphIds = graphIds;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopIdSet getGraphIds() {
    return graphIds;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addGraphId(GradoopId graphId) {
    if (graphIds == null) {
      graphIds = new GradoopIdSet();
    }
    graphIds.add(graphId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setGraphIds(GradoopIdSet graphIds) {
    this.graphIds = graphIds;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void resetGraphIds() {
    if (graphIds != null) {
      graphIds.clear();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getGraphCount() {
    return (graphIds != null) ? graphIds.size() : 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "EPGMGraphElement{" +
      super.toString() +
      ", graphIds=" + graphIds +
      '}';
  }
}
