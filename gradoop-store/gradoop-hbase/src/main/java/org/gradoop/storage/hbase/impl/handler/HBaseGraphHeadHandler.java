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
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.storage.common.predicate.query.ElementQuery;
import org.gradoop.storage.hbase.impl.constants.HBaseConstants;
import org.gradoop.storage.hbase.impl.predicate.filter.api.HBaseElementFilter;
import org.gradoop.storage.hbase.impl.api.GraphHeadHandler;

import java.io.IOException;

/**
 * Used to read/write EPGM graph data from/to a HBase table.
 *
 * Graph data in HBase:
 *
 * |---------|-------------|----------|
 * | row-key | meta        | data     |
 * |---------|-------------|----------|
 * | "0"     | label       | k1  | k2 |
 * |         |-------------|----------|
 * |         | "Community" | v1  | v2 |
 * |---------|-------------|----------|
 */
public class HBaseGraphHeadHandler extends HBaseElementHandler implements GraphHeadHandler {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * Creates graph data objects from the rows.
   */
  private final GraphHeadFactory<EPGMGraphHead> graphHeadFactory;

  /**
   * An optional query to define predicates for the graph store.
   */
  private ElementQuery<HBaseElementFilter<EPGMGraphHead>> graphQuery;

  /**
   * Creates a graph handler.
   *
   * @param graphHeadFactory used to create runtime graph data objects
   */
  public HBaseGraphHeadHandler(GraphHeadFactory<EPGMGraphHead> graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  @Override
  public void createTable(final Admin admin, final HTableDescriptor tableDescriptor)
    throws IOException {
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseConstants.CF_META));
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseConstants.CF_PROPERTY_TYPE));
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseConstants.CF_PROPERTY_VALUE));
    admin.createTable(tableDescriptor);
  }

  @Override
  public Put writeGraphHead(final Put put, final GraphHead graphData) {
    writeLabel(put, graphData);
    writeProperties(put, graphData);
    return put;
  }

  @Override
  public EPGMGraphHead readGraphHead(final Result res) {
    return graphHeadFactory.initGraphHead(readId(res), readLabel(res), readProperties(res));
  }

  @Override
  public GraphHeadHandler applyQuery(ElementQuery<HBaseElementFilter<EPGMGraphHead>> query) {
    this.graphQuery = query;
    return this;
  }

  @Override
  public ElementQuery<HBaseElementFilter<EPGMGraphHead>> getQuery() {
    return this.graphQuery;
  }
}
