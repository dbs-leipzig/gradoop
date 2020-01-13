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
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.storage.common.predicate.query.ElementQuery;
import org.gradoop.storage.hbase.impl.constants.HBaseConstants;
import org.gradoop.storage.hbase.impl.predicate.filter.api.HBaseElementFilter;
import org.gradoop.storage.hbase.impl.api.VertexHandler;
import org.gradoop.storage.hbase.utils.RegionSplitter;

import java.io.IOException;

/**
 * Used to read/write EPGM vertex data from/to a HBase table.
 * <p>
 * Vertex data in HBase:
 * <p>
 * |---------|--------------------|---------|
 * | row-key | meta               | data    |
 * |---------|----------|---------|---------|
 * | "0"     | label    | graphs  | k1 | k2 |
 * |         |----------|---------|----|----|
 * |         | "Person" |  [0,2]  | v1 | v2 |
 * |---------|----------|---------|----|----|
 */
public class HBaseVertexHandler extends HBaseGraphElementHandler implements VertexHandler {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * Creates vertex data objects from the rows.
   */
  private final VertexFactory<EPGMVertex> vertexFactory;

  /**
   * An optional query to define predicates for the graph store.
   */
  private ElementQuery<HBaseElementFilter<EPGMVertex>> vertexQuery;

  /**
   * Creates a vertex handler.
   *
   * @param vertexFactory used to create runtime vertex data objects
   */
  public HBaseVertexHandler(VertexFactory<EPGMVertex> vertexFactory) {
    this.vertexFactory = vertexFactory;
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
  public Put writeVertex(Put put, Vertex vertexData) {
    writeLabel(put, vertexData);
    writeProperties(put, vertexData);
    writeGraphIds(put, vertexData);
    return put;
  }

  @Override
  public EPGMVertex readVertex(final Result res) {
    return vertexFactory.initVertex(readId(res), readLabel(res), readProperties(res),
      readGraphIds(res));
  }

  @Override
  public VertexHandler applyQuery(ElementQuery<HBaseElementFilter<EPGMVertex>> query) {
    this.vertexQuery = query;
    return this;
  }

  @Override
  public ElementQuery<HBaseElementFilter<EPGMVertex>> getQuery() {
    return this.vertexQuery;
  }
}
