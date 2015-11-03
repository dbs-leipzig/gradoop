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

import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.util.GConstants;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.storage.api.GraphElementHandler;

import java.util.Set;

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
  public Put writeGraphs(Put put, EPGMGraphElement graphElement) {
    int graphCount = graphElement.getGraphCount();
    if (graphCount > 0) {
      byte[] graphs =
        new byte[Bytes.SIZEOF_INT + graphCount * Bytes.SIZEOF_LONG];
      Bytes.putInt(graphs, 0, graphCount);
      int offset = Bytes.SIZEOF_INT;
      for (Long graphId : graphElement.getGraphs()) {
        Bytes.putLong(graphs, offset, graphId);
        offset += Bytes.SIZEOF_LONG;
      }
      put = put.add(CF_META_BYTES, COL_GRAPHS_BYTES, graphs);
    }
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<Long> readGraphs(Result res) {
    byte[] graphBytes = res.getValue(CF_META_BYTES, COL_GRAPHS_BYTES);
    Set<Long> result = null;
    if (graphBytes != null) {
      int graphCount = Bytes.toInt(graphBytes);
      result = Sets.newHashSetWithExpectedSize(graphCount);
      int offset = Bytes.SIZEOF_INT;
      for (int i = 0; i < graphCount; i++) {
        result.add(Bytes.toLong(graphBytes, offset));
        offset += Bytes.SIZEOF_LONG;
      }
    }
    return result;
  }
}
