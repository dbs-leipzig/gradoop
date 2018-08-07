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
package org.gradoop.storage.impl.hbase.handler;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.storage.common.predicate.query.ElementQuery;
import org.gradoop.storage.impl.hbase.api.EdgeHandler;
import org.gradoop.storage.impl.hbase.constants.HBaseConstants;
import org.gradoop.storage.impl.hbase.predicate.filter.api.HBaseElementFilter;

import java.io.IOException;

/**
 * Used to read/write EPGM edge data from/to a HBase table.
 * <p>
 * EPGMEdge data in HBase:
 * <p>
 * |---------|---------------------------------------------|-------|
 * | row-key | meta                                        | data  |
 * |---------|----------|------------|------------|--------|-------|
 * | "0"     | label    | source     | target     | graphs | since |
 * |         |----------|------------|------------|--------|-------|
 * |         | "knows"  | <Person.0> | <Person.1> | [0,1]  | 2014  |
 * |---------|----------|------------|------------|--------|-------|
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
  private final EPGMEdgeFactory<Edge> edgeFactory;

  /**
   * An optional query to define predicates for the graph store.
   */
  private ElementQuery<HBaseElementFilter<Edge>> edgeQuery;

  /**
   * Creates an edge data handler.
   *
   * @param edgeFactory edge data factory
   */
  public HBaseEdgeHandler(EPGMEdgeFactory<Edge> edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createTable(final Admin admin, final HTableDescriptor tableDescriptor)
    throws IOException {
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseConstants.CF_META));
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseConstants.CF_PROPERTY_TYPE));
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseConstants.CF_PROPERTY_VALUE));
    admin.createTable(tableDescriptor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeSource(final Put put, final GradoopId sourceId) {
    return put.addColumn(CF_META_BYTES, COL_SOURCE_BYTES, sourceId.toByteArray());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId readSourceId(Result res) {
    return GradoopId.fromByteArray(res.getValue(CF_META_BYTES, COL_SOURCE_BYTES));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeTarget(Put put, GradoopId targetId) {
    return put.addColumn(CF_META_BYTES, COL_TARGET_BYTES, targetId.toByteArray());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId readTargetId(Result res) {
    return GradoopId.fromByteArray(res.getValue(CF_META_BYTES, COL_TARGET_BYTES));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeEdge(Put put, EPGMEdge edgeData) {
    writeLabel(put, edgeData);
    writeSource(put, edgeData.getSourceId());
    writeTarget(put, edgeData.getTargetId());
    writeProperties(put, edgeData);
    writeGraphIds(put, edgeData);
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge readEdge(Result res) {
    return edgeFactory.initEdge(readId(res), readLabel(res), readSourceId(res), readTargetId(res),
        readProperties(res), readGraphIds(res));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EdgeHandler applyQuery(ElementQuery<HBaseElementFilter<Edge>> query) {
    this.edgeQuery = query;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ElementQuery<HBaseElementFilter<Edge>> getQuery() {
    return this.edgeQuery;
  }
}
