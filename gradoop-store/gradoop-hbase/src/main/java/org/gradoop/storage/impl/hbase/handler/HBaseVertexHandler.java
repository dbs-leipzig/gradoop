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
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.storage.common.predicate.query.ElementQuery;
import org.gradoop.storage.impl.hbase.api.VertexHandler;
import org.gradoop.storage.impl.hbase.constants.HBaseConstants;
import org.gradoop.storage.impl.hbase.predicate.filter.api.HBaseElementFilter;

import java.io.IOException;

/**
 * Used to read/write EPGM vertex data from/to a HBase table.
 * <p>
 * EPGMVertex data in HBase:
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
  private final EPGMVertexFactory<Vertex> vertexFactory;

  /**
   * An optional query to define predicates for the graph store.
   */
  private ElementQuery<HBaseElementFilter<Vertex>> vertexQuery;

  /**
   * Creates a vertex handler.
   *
   * @param vertexFactory used to create runtime vertex data objects
   */
  public HBaseVertexHandler(EPGMVertexFactory<Vertex> vertexFactory) {
    this.vertexFactory = vertexFactory;
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
  public Put writeVertex(Put put, EPGMVertex vertexData) {
    writeLabel(put, vertexData);
    writeProperties(put, vertexData);
    writeGraphIds(put, vertexData);
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex readVertex(final Result res) {
    return vertexFactory.initVertex(readId(res), readLabel(res), readProperties(res),
      readGraphIds(res));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexHandler applyQuery(ElementQuery<HBaseElementFilter<Vertex>> query) {
    this.vertexQuery = query;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ElementQuery<HBaseElementFilter<Vertex>> getQuery() {
    return this.vertexQuery;
  }
}
