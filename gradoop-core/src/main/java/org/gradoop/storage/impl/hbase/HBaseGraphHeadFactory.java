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
import org.gradoop.model.impl.id.GradoopIds;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.storage.api.PersistentGraphHeadFactory;

/**
 * Default factory for creating persistent graph data representation.
 */
public class HBaseGraphHeadFactory implements
  PersistentGraphHeadFactory<GraphHeadPojo, HBaseGraphHead> {

  /**
   * Logger
   */
  private static final Logger LOG =
    Logger.getLogger(HBaseGraphHeadFactory.class);
  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public HBaseGraphHead createGraphHead(GraphHeadPojo inputGraphData,
    GradoopIds vertices, GradoopIds edges) {
    HBaseGraphHead defaultPersistentGraphData =
      new HBaseGraphHead(inputGraphData, vertices, edges);
    LOG.info("Created persistent graph data: " + defaultPersistentGraphData);
    return defaultPersistentGraphData;
  }
}
