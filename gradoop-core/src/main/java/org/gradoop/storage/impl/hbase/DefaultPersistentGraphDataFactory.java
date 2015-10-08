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

import org.apache.log4j.Logger;
import org.gradoop.model.impl.pojo.DefaultGraphData;
import org.gradoop.storage.api.PersistentGraphDataFactory;

import java.util.Set;

/**
 * Default factory for creating persistent graph data representation.
 */
public class DefaultPersistentGraphDataFactory implements
  PersistentGraphDataFactory<DefaultGraphData, DefaultPersistentGraphData> {

  /**
   * Logger
   */
  private static final Logger LOG =
    Logger.getLogger(DefaultPersistentGraphDataFactory.class);
  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public DefaultPersistentGraphData createGraphData(
    DefaultGraphData inputGraphData, Set<Long> vertices, Set<Long> edges) {
    DefaultPersistentGraphData defaultPersistentGraphData =
      new DefaultPersistentGraphData(inputGraphData, vertices, edges);
    LOG.info("Created persistent graph data: " + defaultPersistentGraphData);
    return defaultPersistentGraphData;
  }
}
