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
package org.gradoop.storage.hbase.impl.handler;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.storage.common.predicate.query.ElementQuery;
import org.gradoop.storage.hbase.impl.constants.HBaseConstants;
import org.gradoop.storage.hbase.impl.predicate.filter.api.HBaseElementFilter;
import org.gradoop.storage.hbase.impl.api.EdgeHandler;
import org.gradoop.storage.hbase.utils.RegionSplitter;

import java.io.IOException;

/**
 * Used to read/write EPGM edge data from/to a HBase table.
 * <table border="1">
 *   <caption>Edge data in HBase</caption>
 *   <tr>
 *     <td>row-key</td>
 *     <td colspan="3">meta</td>
 *     <td>data</td>
 *   </tr>
 *   <tr>
 *     <td rowspan="2">"0"</td>
 *     <td>label</td>
 *     <td>source</td>
 *     <td>target</td>
 *     <td>graphs</td>
 *     <td>since</td>
 *   </tr>
 *   <tr>
 *     <td>"knows"</td>
 *     <td>{@code <Person.0>}</td>
 *     <td>{@code <Person.1>}</td>
 *     <td>[0,1]</td>
 *     <td>2014</td>
 *   </tr>
 * </table>
 */
public class HBaseEdgeHandler extends HBaseGraphElementHandler implements EdgeHandler {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * Byte array representation of the source vertex column identifier.
   */
  private static final byte[] COL_SOURCE_BYTES = Bytes.toBytes(HBaseConstants.COL_SOURCE);
  /**
   * Byte array representation of the target vertex column identifier.
   */
  private static final byte[] COL_TARGET_BYTES = Bytes.toBytes(HBaseConstants.COL_TARGET);

  /**
   * Creates edge data objects from the rows.
   */
  private final EdgeFactory<EPGMEdge> edgeFactory;

  /**
   * An optional query to define predicates for the graph store.
   */
  private ElementQuery<HBaseElementFilter<EPGMEdge>> edgeQuery;

  /**
   * Creates an edge data handler.
   *
   * @param edgeFactory edge data factory
   */
  public HBaseEdgeHandler(EdgeFactory<EPGMEdge> edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  @Override
  public void createTable(final Admin admin, final HTableDescriptor tableDescriptor)
    throws IOException {
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseConstants.CF_META));
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseConstants.CF_PROPERTY_TYPE));
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseConstants.CF_PROPERTY_VALUE));
    if (isPreSplitRegions()) {
      admin.createTable(
        tableDescriptor,
        RegionSplitter.getInstance().getStartKey(),
        RegionSplitter.getInstance().getEndKey(),
        RegionSplitter.getInstance().getNumberOfRegions());
    } else {
      admin.createTable(tableDescriptor);
    }
  }

  @Override
  public Put writeSource(final Put put, final GradoopId sourceId) {
    return put.addColumn(CF_META_BYTES, COL_SOURCE_BYTES, sourceId.toByteArray());
  }

  @Override
  public GradoopId readSourceId(Result res) {
    return GradoopId.fromByteArray(res.getValue(CF_META_BYTES, COL_SOURCE_BYTES));
  }

  @Override
  public Put writeTarget(Put put, GradoopId targetId) {
    return put.addColumn(CF_META_BYTES, COL_TARGET_BYTES, targetId.toByteArray());
  }

  @Override
  public GradoopId readTargetId(Result res) {
    return GradoopId.fromByteArray(res.getValue(CF_META_BYTES, COL_TARGET_BYTES));
  }

  @Override
  public Put writeEdge(Put put, Edge edgeData) {
    writeLabel(put, edgeData);
    writeSource(put, edgeData.getSourceId());
    writeTarget(put, edgeData.getTargetId());
    writeProperties(put, edgeData);
    writeGraphIds(put, edgeData);
    return put;
  }

  @Override
  public EPGMEdge readEdge(Result res) {
    return edgeFactory.initEdge(readId(res), readLabel(res), readSourceId(res), readTargetId(res),
        readProperties(res), readGraphIds(res));
  }

  @Override
  public EdgeHandler applyQuery(ElementQuery<HBaseElementFilter<EPGMEdge>> query) {
    this.edgeQuery = query;
    return this;
  }

  @Override
  public ElementQuery<HBaseElementFilter<EPGMEdge>> getQuery() {
    return this.edgeQuery;
  }
}
