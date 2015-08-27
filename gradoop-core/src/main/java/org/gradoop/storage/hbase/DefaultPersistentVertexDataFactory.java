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
import org.gradoop.storage.PersistentVertexDataFactory;

import java.util.Set;

/**
 * Default factory for creating persistent vertex data representation.
 */
public class DefaultPersistentVertexDataFactory implements
  PersistentVertexDataFactory<DefaultVertexData, DefaultEdgeData,
    DefaultPersistentVertexData> {

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
  public DefaultPersistentVertexData createVertexData(
    DefaultVertexData inputVertexData, Set<DefaultEdgeData> outgoingEdgeIds,
    Set<DefaultEdgeData> incomingEdgeIds) {
    DefaultPersistentVertexData defaultPersistentVertexData =
      new DefaultPersistentVertexData(inputVertexData, outgoingEdgeIds,
        incomingEdgeIds);
    LOG.info("Created persistent vertex data: " + defaultPersistentVertexData);
    return defaultPersistentVertexData;
  }
}
