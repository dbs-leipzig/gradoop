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

import org.apache.log4j.Logger;
import org.gradoop.model.impl.DefaultEdgeData;
import org.gradoop.model.impl.DefaultVertexData;
import org.gradoop.storage.PersistentEdgeDataFactory;

/**
 * Default factory for creating persistent edge data representation.
 */
public class DefaultPersistentEdgeDataFactory implements
  PersistentEdgeDataFactory<DefaultEdgeData, DefaultVertexData,
    DefaultPersistentEdgeData> {

  /**
   * Logger
   */
  private static final Logger LOG =
    Logger.getLogger(DefaultPersistentVertexDataFactory.class);
  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public DefaultPersistentEdgeData createEdgeData(DefaultEdgeData inputEdgeData,
    DefaultVertexData sourceVertexData, DefaultVertexData targetVertexData) {
    DefaultPersistentEdgeData defaultPersistentEdgeData =
      new DefaultPersistentEdgeData(inputEdgeData, sourceVertexData,
        targetVertexData);
    LOG.info("Created persistent edge data: " + defaultPersistentEdgeData);
    return defaultPersistentEdgeData;
  }
}
