/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.storage.hbase.impl.io.inputformats;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.storage.common.api.EPGMGraphOutput;
import org.gradoop.storage.hbase.impl.api.GraphHeadHandler;

/**
 * Reads graph data from HBase.
 */
public class GraphHeadTableInputFormat extends BaseTableInputFormat<EPGMGraphHead> {

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
   * Get the scanner instance. If a query was applied to the elementHandler,
   * the Scan will be extended with a HBase filter representation of that query.
   *
   * @return the Scan instance with an optional HBase filter applied
   */
  @Override
  protected Scan getScanner() {
    Scan scan = new Scan();
    scan.setCaching(EPGMGraphOutput.DEFAULT_CACHE_SIZE);

    if (graphHeadHandler.getQuery() != null) {
      attachFilter(graphHeadHandler.getQuery(), scan, graphHeadHandler.isSpreadingByteUsed());
    }

    return scan;
  }

  @Override
  protected String getTableName() {
    return graphHeadTableName;
  }

  @Override
  protected Tuple1<EPGMGraphHead> mapResultToTuple(Result result) {
    return new Tuple1<>(graphHeadHandler.readGraphHead(result));
  }
}
