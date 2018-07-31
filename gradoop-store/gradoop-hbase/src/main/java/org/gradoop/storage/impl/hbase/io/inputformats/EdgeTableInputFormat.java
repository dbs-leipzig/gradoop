/*
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
package org.gradoop.storage.impl.hbase.io.inputformats;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.storage.common.api.EPGMGraphOutput;
import org.gradoop.storage.impl.hbase.api.EdgeHandler;

/**
 * Reads edge data from HBase.
 */
public class EdgeTableInputFormat extends BaseTableInputFormat<Edge> {

  /**
   * Handles reading of persistent edge data.
   */
  private final EdgeHandler edgeHandler;

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
  public EdgeTableInputFormat(EdgeHandler edgeHandler, String edgeTableName) {
    this.edgeHandler = edgeHandler;
    this.edgeTableName = edgeTableName;
  }

  /**
   * Get the scanner instance. If a query was applied to the elementHandler,
   * the Scan will be extended with a HBase filter representation of that query.
   *
   * @return the Scan instance with an optional HBase filter applied
   */
  @Override
  protected Scan getScanner() {
    Scan scan = new Scan();
    scan.setCaching(EPGMGraphOutput.DEFAULT_CACHE_SIZE);

    if (edgeHandler.getQuery() != null) {
      attachFilter(edgeHandler.getQuery(), scan);
    }

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
  protected Tuple1<Edge> mapResultToTuple(Result result) {
    return new Tuple1<>(edgeHandler.readEdge(result));
  }
}
