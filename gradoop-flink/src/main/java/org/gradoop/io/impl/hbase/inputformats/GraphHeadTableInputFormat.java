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

package org.gradoop.io.impl.hbase.inputformats;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.gradoop.model.api.epgm.GraphHead;
import org.gradoop.storage.api.GraphHeadHandler;
import org.gradoop.util.GConstants;

/**
 * Reads graph data from HBase.
 */
public class GraphHeadTableInputFormat
  extends TableInputFormat<Tuple1<GraphHead>> {

  /**
   * Handles reading of persistent graph data.
   */
  private final GraphHeadHandler graphHeadHandler;

  /**
   * Table to read from.
   */
  private final String graphHeadTableName;

  /**
   * Creates an graph table input format.
   *
   * @param graphHeadHandler   graph data handler
   * @param graphHeadTableName graph data table name
   */
  public GraphHeadTableInputFormat(GraphHeadHandler graphHeadHandler,
    String graphHeadTableName) {
    this.graphHeadHandler = graphHeadHandler;
    this.graphHeadTableName = graphHeadTableName;
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
    return graphHeadTableName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Tuple1<GraphHead> mapResultToTuple(Result result) {
    return new Tuple1<>(graphHeadHandler.readGraphHead(result));
  }
}
