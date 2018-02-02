/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.io.impl.hbase.inputformats;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.storage.api.EdgeHandler;
import org.gradoop.common.util.HBaseConstants;

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
    scan.setCaching(HBaseConstants.HBASE_DEFAULT_SCAN_CACHE_SIZE);
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
