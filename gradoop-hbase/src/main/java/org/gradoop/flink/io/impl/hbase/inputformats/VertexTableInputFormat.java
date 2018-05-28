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
import org.gradoop.common.storage.api.VertexHandler;
import org.gradoop.common.util.HBaseConstants;

/**
 * Reads vertex data from HBase.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class VertexTableInputFormat<V extends EPGMVertex, E extends EPGMEdge>
  extends TableInputFormat<Tuple1<V>> {

  /**
   * Handles reading of persistent vertex data.
   */
  private final VertexHandler<V, E> vertexHandler;

  /**
   * Table to read from.
   */
  private final String vertexTableName;

  /**
   * Creates an vertex table input format.
   *
   * @param vertexHandler   vertex data handler
   * @param vertexTableName vertex data table name
   */
  public VertexTableInputFormat(VertexHandler<V, E> vertexHandler,
    String vertexTableName) {
    this.vertexHandler = vertexHandler;
    this.vertexTableName = vertexTableName;
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
    return vertexTableName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Tuple1<V> mapResultToTuple(Result result) {
    return new Tuple1<>(vertexHandler.readVertex(result));
  }
}
