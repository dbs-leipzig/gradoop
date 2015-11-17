/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.storage.impl.hbase;

import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIds;

/**
 * Wraps an EPGM graph element data entity.
 *
 * @param <T> entity type
 */
public abstract class HBaseGraphElement<T extends EPGMGraphElement> extends
  HBaseElement<T> implements EPGMGraphElement {

  /**
   * Default constructor.
   */
  protected HBaseGraphElement() {
  }

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
  public GradoopIds getGraphIds() {
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
  public void setGraphIds(GradoopIds graphIds) {
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
