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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.common.model.impl.pojo;

import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Abstract class representing an EPGM element that is contained in a logical
 * graph (i.e. vertices and edges).
 */
public abstract class GraphElement
  extends Element
  implements EPGMGraphElement {

  /**
   * Set of graph identifiers that element is contained in
   */
  private GradoopIdList graphIds;

  /**
   * Default constructor.
   */
  protected GraphElement() {
  }

  /**
   * Creates an EPGM graph element using the given arguments.
   *  @param id         element id
   * @param label      element label
   * @param properties element properties
   * @param graphIds     graphIds that element is contained in
   */
  protected GraphElement(GradoopId id, String label,
    Properties properties, GradoopIdList graphIds) {
    super(id, label, properties);
    this.graphIds = graphIds;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopIdList getGraphIds() {
    return graphIds;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addGraphId(GradoopId graphId) {
    if (graphIds == null) {
      graphIds = new GradoopIdList();
    }
    graphIds.add(graphId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setGraphIds(GradoopIdList graphIds) {
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
    return String.format("%s @ %s", super.toString(), graphIds);
  }
}
