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
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.storage.impl.hbase.api.EdgeHandler;
import org.gradoop.common.storage.impl.hbase.constants.HBaseConstants;

import java.util.ArrayList;
import java.util.List;

/**
 * Reads edge data from HBase.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class EdgeTableInputFormat<E extends EPGMEdge, V extends EPGMVertex>
  extends TableInputFormat<Tuple1<E>> {

  /**
   * An optional set of vertex ids to define a filter for HBase.
   */
  private GradoopIdSet filterVertexIds = new GradoopIdSet();

  /**
   * An optional set of edge ids to define a filter for HBase.
   */
  private GradoopIdSet filterEdgeIds = new GradoopIdSet();

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

    FilterList conjunctFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);

    // if vertex ids are given, add a filter
    if (filterVertexIds != null && !filterVertexIds.isEmpty()) {
      conjunctFilters.addFilter(getVertexIdFilter(filterVertexIds));
    }

    // if edge ids are given, add a filter
    if (filterEdgeIds != null && !filterEdgeIds.isEmpty()) {
      conjunctFilters.addFilter(getEdgeIdFilter(filterEdgeIds));
    }

    // if there are filters inside the root list, add it to the Scan object
    if (!conjunctFilters.getFilters().isEmpty()) {
      scan.setFilter(conjunctFilters);
    }

    return scan;
  }

  /**
   * Creates a HBase Filter object to return only edges with vertices
   * as source and target which are identified by the given vertex GradoopIds.
   *
   * @param vertexIds a set of vertex GradoopIds to filter
   * @return a HBase Filter object
   */
  private Filter getVertexIdFilter(GradoopIdSet vertexIds) {
    FilterList conjunctFilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    List<Filter> srcFilter = new ArrayList<>();
    List<Filter> trgFilter = new ArrayList<>();
    SingleColumnValueFilter tmpSrcFilter;
    SingleColumnValueFilter tmpTargetFilter;

    for (GradoopId gradoopId : vertexIds) {
      BinaryComparator rowKeyComparator = new BinaryComparator(gradoopId.toByteArray());

      tmpSrcFilter = new SingleColumnValueFilter(
        Bytes.toBytesBinary(HBaseConstants.CF_META),
        Bytes.toBytesBinary(HBaseConstants.COL_SOURCE),
        CompareFilter.CompareOp.EQUAL,
        rowKeyComparator
      );
      srcFilter.add(tmpSrcFilter);

      tmpTargetFilter = new SingleColumnValueFilter(
        Bytes.toBytesBinary(HBaseConstants.CF_META),
        Bytes.toBytesBinary(HBaseConstants.COL_TARGET),
        CompareFilter.CompareOp.EQUAL,
        rowKeyComparator
      );
      trgFilter.add(tmpTargetFilter);
    }

    // create two disjunctive filter lists, one for source, one for target vertices and add them
    // to the conjunctive filter list
    conjunctFilterList.addFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE, srcFilter));
    conjunctFilterList.addFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE, trgFilter));

    return conjunctFilterList;
  }

  /**
   * Creates a HBase Filter object to return only edges with the given GradoopIds.
   *
   * @param edgeIds a set of edge GradoopIds to filter
   * @return a HBase Filter object
   */
  private Filter getEdgeIdFilter(GradoopIdSet edgeIds) {
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

    for (GradoopId gradoopId : edgeIds) {
      RowFilter rowFilter = new RowFilter(
        CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(gradoopId.toByteArray())
      );
      filterList.addFilter(rowFilter);
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
