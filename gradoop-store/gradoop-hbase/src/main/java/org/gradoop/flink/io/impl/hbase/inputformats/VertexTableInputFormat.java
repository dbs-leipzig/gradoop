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
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.storage.impl.hbase.api.VertexHandler;
import org.gradoop.common.storage.impl.hbase.constants.HBaseConstants;
import org.gradoop.common.storage.impl.hbase.predicate.filter.HBaseFilterUtils;

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
   * Get the scanner instance. If a query was applied to the elementHandler,
   * the Scan will be extended with a HBase filter representation of that query.
   *
   * @return the Scan instance with an optional HBase filter applied
   */
  @Override
  protected Scan getScanner() {
    Scan scan = new Scan();
    scan.setCaching(HBaseConstants.HBASE_DEFAULT_SCAN_CACHE_SIZE);

    if (vertexHandler.getQuery() != null) {
      FilterList conjunctFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);

      if (vertexHandler.getQuery().getQueryRanges() != null &&
        !vertexHandler.getQuery().getQueryRanges().isEmpty()) {
        conjunctFilters.addFilter(
          HBaseFilterUtils.getIdFilter(vertexHandler.getQuery().getQueryRanges())
        );
      }

      if (vertexHandler.getQuery().getFilterPredicate() != null) {
        conjunctFilters.addFilter(vertexHandler.getQuery().getFilterPredicate().toHBaseFilter());
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
