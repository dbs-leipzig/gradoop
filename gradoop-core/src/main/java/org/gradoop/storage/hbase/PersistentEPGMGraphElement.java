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

package org.gradoop.storage.hbase;

import org.gradoop.model.GraphElement;

import java.util.Set;

/**
 * Wraps an EPGM graph element data entity.
 *
 * @param <T> entity type
 */
public abstract class PersistentEPGMGraphElement<T extends GraphElement> extends
  PersistentEPGMElement<T> implements GraphElement {

  /**
   * Default constructor.
   */
  protected PersistentEPGMGraphElement() {
  }

  /**
   * Creates an EPGM graph element.
   *
   * @param epgmGraphElement encapsulated graph element
   */
  protected PersistentEPGMGraphElement(T epgmGraphElement) {
    super(epgmGraphElement);
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public Set<Long> getGraphs() {
    return getEpgmElement().getGraphs();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addGraph(Long graph) {
    getEpgmElement().addGraph(graph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setGraphs(Set<Long> graphs) {
    getEpgmElement().setGraphs(graphs);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void resetGraphs() {
    getEpgmElement().resetGraphs();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getGraphCount() {
    return getEpgmElement().getGraphCount();
  }
}
