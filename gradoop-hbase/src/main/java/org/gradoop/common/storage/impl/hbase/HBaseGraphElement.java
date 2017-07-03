
package org.gradoop.common.storage.impl.hbase;

import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Wraps an EPGM graph element data entity.
 *
 * @param <T> entity type
 */
public abstract class HBaseGraphElement<T extends EPGMGraphElement>
  extends HBaseElement<T> implements EPGMGraphElement {

  /**
   * Creates an EPGM graph element.
   *
   * @param epgmGraphElement encapsulated graph element
   */
  protected HBaseGraphElement(T epgmGraphElement) {
    super(epgmGraphElement);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopIdList getGraphIds() {
    return getEpgmElement().getGraphIds();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addGraphId(GradoopId graphId) {
    getEpgmElement().addGraphId(graphId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setGraphIds(GradoopIdList graphIds) {
    getEpgmElement().setGraphIds(graphIds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void resetGraphIds() {
    getEpgmElement().resetGraphIds();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getGraphCount() {
    return getEpgmElement().getGraphCount();
  }
}
