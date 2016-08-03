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

package org.gradoop.flink.io.impl.hbase.inputformats;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.storage.api.EdgeHandler;
import org.gradoop.common.util.GConstants;

/**
 * Reads edge data from HBase.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class EdgeTableInputFormat<E extends EPGMEdge, V extends EPGMVertex>
  extends TableInputFormat<Tuple1<E>> {

  /**
   * Handles reading of persistent edge data.
   */
  private final EdgeHandler<E, V> edgeHandler;

  /**
   * Table to read from.
   */
  private final String edgeTableName;

  /**
   * Creates an edge table input format.
   *
   * @param edgeHandler   edge data handler
   * @param edgeTableName edge data table name
   */
  public EdgeTableInputFormat(EdgeHandler<E, V> edgeHandler,
    String edgeTableName) {
    this.edgeHandler = edgeHandler;
    this.edgeTableName = edgeTableName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Scan getScanner() {
    Scan scan = new Scan();
    scan.setCaching(GConstants.HBASE_DEFAULT_SCAN_CACHE_SIZE);
    return scan;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getTableName() {
    return edgeTableName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Tuple1<E> mapResultToTuple(Result result) {
    return new Tuple1<>(edgeHandler.readEdge(result));
  }
}
