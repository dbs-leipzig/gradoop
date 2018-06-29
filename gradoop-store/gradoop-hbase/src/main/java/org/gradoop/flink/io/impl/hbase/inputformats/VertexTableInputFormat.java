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
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.storage.impl.hbase.api.VertexHandler;
import org.gradoop.common.storage.impl.hbase.constants.HBaseConstants;

/**
 * Reads vertex data from HBase.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class VertexTableInputFormat<V extends EPGMVertex, E extends EPGMEdge>
  extends TableInputFormat<Tuple1<V>> {

  /**
   * An optional set of vertex ids to define a filter for HBase.
   */
  private GradoopIdSet filterVertexIds = new GradoopIdSet();

  /**
   * An optional set of edge ids to define a filter for HBase.
   */
  private GradoopIdSet filterEdgeIds = new GradoopIdSet();

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

    FilterList conjunctFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);

    // if vertex ids are given, add a filter
    if (!filterVertexIds.isEmpty()) {
      conjunctFilters.addFilter(getVertexIdFilter(filterVertexIds));
    }

    // if edge ids are given, add a filter
    if (!filterEdgeIds.isEmpty()) {
      conjunctFilters.addFilter(getEdgeIdFilter(filterEdgeIds));
    }

    // if there are filters inside the root list, add it to the Scan object
    if (!conjunctFilters.getFilters().isEmpty()) {
      scan.setFilter(conjunctFilters);
    }

    return scan;
  }

  /**
   * Creates a HBase Filter object to return only vertices with the given GradoopIds.
   *
   * @param vertexIds a set of vertex GradoopIds to filter
   * @return a HBase Filter object
   */
  private Filter getVertexIdFilter(GradoopIdSet vertexIds) {
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    for (GradoopId vertexId : vertexIds) {
      RowFilter rowFilter = new RowFilter(
        CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(vertexId.toByteArray())
      );
      filterList.addFilter(rowFilter);
    }

    return filterList;
  }

  /**
   * Creates a HBase Filter object to return only edges with vertices
   * as source and target that are identified by the given GradoopIds.
   *
   * @param edgeIds a set of edge GradoopIds to filter
   * @return a HBase Filter object
   */
  private Filter getEdgeIdFilter(GradoopIdSet edgeIds) {
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    BinaryComparator nullStringComparator = new BinaryComparator(Bytes.toBytes(""));
    SingleColumnValueFilter ieFilter;
    SingleColumnValueFilter oeFilter;

    for (GradoopId gradoopId : edgeIds) {
      byte[] edgeIdBytes = gradoopId.toByteArray();

      ieFilter = new SingleColumnValueExcludeFilter(
        Bytes.toBytesBinary(HBaseConstants.CF_IN_EDGES),
        edgeIdBytes,
        CompareFilter.CompareOp.EQUAL,
        nullStringComparator
      );
      // if row does not have this column, exclude them
      ieFilter.setFilterIfMissing(true);

      filterList.addFilter(ieFilter);

      oeFilter = new SingleColumnValueExcludeFilter(
        Bytes.toBytesBinary(HBaseConstants.CF_OUT_EDGES),
        edgeIdBytes,
        CompareFilter.CompareOp.EQUAL,
        nullStringComparator
      );
      // if row does not have this column, exclude them
      oeFilter.setFilterIfMissing(true);

      filterList.addFilter(oeFilter);
    }

    return filterList;
  }

  /**
   * Setter for GradoopIds of vertices to filter. Note that this implies the
   * extension of the Scan object returned by getScanner() with HBase filters.
   *
   * @param filterVertexIds a GradoopIdSet of vertex ids
   */
  public void setFilterVertexIds(GradoopIdSet filterVertexIds) {
    this.filterVertexIds.addAll(filterVertexIds);
  }

  /**
   * Setter for GradoopIds of edges to filter. Note that this implies the
   * extension of the Scan object returned by getScanner() with HBase filters.
   *
   * @param filterEdgeIds a GradoopIdSet of edge ids
   */
  public void setFilterEdgeIds(GradoopIdSet filterEdgeIds) {
    this.filterEdgeIds.addAll(filterEdgeIds);
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
