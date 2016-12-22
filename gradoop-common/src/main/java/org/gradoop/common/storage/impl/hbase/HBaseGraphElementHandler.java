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

package org.gradoop.common.storage.impl.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.storage.api.GraphElementHandler;
import org.gradoop.common.util.GConstants;

import java.io.IOException;

/**
 * Handler class for entities that are contained in logical graphs (i.e.,
 * vertex and edge data).
 */
public abstract class HBaseGraphElementHandler extends
  HBaseElementHandler implements GraphElementHandler {
  /**
   * Byte representation of the graphs column identifier.
   */
  private static final byte[] COL_GRAPHS_BYTES =
    Bytes.toBytes(GConstants.COL_GRAPHS);

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeGraphIds(Put put, EPGMGraphElement graphElement) throws
    IOException {

    if (graphElement.getGraphCount() > 0) {
      put = put.add(CF_META_BYTES, COL_GRAPHS_BYTES, graphElement.getGraphIds().toByteArray());
    }

    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopIdList readGraphIds(Result res) throws IOException {
    byte[] graphBytes = res.getValue(CF_META_BYTES, COL_GRAPHS_BYTES);

    GradoopIdList graphIds;

    if (graphBytes != null) {
      graphIds = GradoopIdList.fromByteArray(graphBytes);
    } else {
      graphIds = new GradoopIdList();
    }

    return graphIds;
  }
}
