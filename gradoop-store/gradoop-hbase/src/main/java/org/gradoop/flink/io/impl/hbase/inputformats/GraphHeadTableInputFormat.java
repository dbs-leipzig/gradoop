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
import org.apache.hadoop.hbase.filter.FilterList;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.storage.impl.hbase.api.GraphHeadHandler;
import org.gradoop.common.storage.impl.hbase.constants.HBaseConstants;
import org.gradoop.common.storage.impl.hbase.predicate.filter.HBaseFilterUtils;

/**
 * Reads graph data from HBase.
 *
 * @param <G> EPGM graph head type
 */
public class GraphHeadTableInputFormat<G extends EPGMGraphHead>
  extends TableInputFormat<Tuple1<G>> {

  /**
   * Handles reading of persistent graph data.
   */
  private final GraphHeadHandler<G> graphHeadHandler;

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
  public GraphHeadTableInputFormat(GraphHeadHandler<G> graphHeadHandler,
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
    scan.setCaching(HBaseConstants.HBASE_DEFAULT_SCAN_CACHE_SIZE);

    if (graphHeadHandler.getQuery() != null) {
      FilterList conjunctFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);

      if (graphHeadHandler.getQuery().getQueryRanges() != null &&
        !graphHeadHandler.getQuery().getQueryRanges().isEmpty()) {
        conjunctFilters.addFilter(
          HBaseFilterUtils.getIdFilter(graphHeadHandler.getQuery().getQueryRanges())
        );
      }

      if (graphHeadHandler.getQuery().getFilterPredicate() != null) {
        conjunctFilters.addFilter(graphHeadHandler.getQuery().getFilterPredicate().toHBaseFilter());
      }

      // if there are filters inside the root list, add it to the Scan object
      if (!conjunctFilters.getFilters().isEmpty()) {
        scan.setFilter(conjunctFilters);
      }
    }

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
  protected Tuple1<G> mapResultToTuple(Result result) {
    return new Tuple1<>(graphHeadHandler.readGraphHead(result));
  }
}
