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
package org.gradoop.common.storage.impl.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.storage.api.GraphHeadHandler;
import org.gradoop.common.storage.api.PersistentGraphHead;
import org.gradoop.common.util.HBaseConstants;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Used to read/write EPGM graph data from/to a HBase table.
 *
 * Graph data in HBase:
 *
 * |---------|-------------|----------|-----------|-----------|
 * | row-key | meta        | data     | vertices  | edges     |
 * |---------|-------------|----------|----- -----|-----------|
 * | "0"     | label       | k1  | k2 | 0 | 1 | 4 | 0 | 1 | 6 |
 * |         |-------------|----------|-----------|---|---|---|
 * |         | "Community" | v1  | v2 |   |   |   |   |   |   |
 * |---------|-------------|----------|-----------|---|---|---|
 *
 * @param <G> EPGM graph head type
 */
public class HBaseGraphHeadHandler<G extends EPGMGraphHead>
  extends HBaseElementHandler
  implements GraphHeadHandler<G> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * Byte array representation of the vertices column family.
   */
  private static final byte[] CF_VERTICES_BYTES = Bytes.toBytes(HBaseConstants.CF_VERTICES);

  /**
   * Byte array representation of the edges column family.
   */
  private static final byte[] CF_EDGES_BYTES = Bytes.toBytes(HBaseConstants.CF_EDGES);

  /**
   * Creates graph data objects from the rows.
   */
  private final EPGMGraphHeadFactory<G> graphHeadFactory;

  /**
   * Creates a graph handler.
   *
   * @param graphHeadFactory used to create runtime graph data objects
   */
  public HBaseGraphHeadHandler(EPGMGraphHeadFactory<G> graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createTable(final HBaseAdmin admin, final HTableDescriptor tableDescriptor)
    throws IOException {
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseConstants.CF_META));
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseConstants.CF_PROPERTIES));
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseConstants.CF_VERTICES));
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseConstants.CF_EDGES));
    admin.createTable(tableDescriptor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeVertices(final Put put, final PersistentGraphHead graphData) throws IOException {
    for (GradoopId vertexId : graphData.getVertexIds()) {
      put.add(CF_VERTICES_BYTES, vertexId.toByteArray(), null);
    }
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<Long> readVertices(final Result res) {
    return getColumnKeysFromFamily(res, CF_VERTICES_BYTES);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeEdges(Put put, PersistentGraphHead graphData) throws IOException {
    for (GradoopId edgeId : graphData.getEdgeIds()) {
      put.add(CF_EDGES_BYTES, edgeId.toByteArray(), null);
    }
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<Long> readEdges(Result res) {
    return getColumnKeysFromFamily(res, CF_EDGES_BYTES);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeGraphHead(final Put put, final PersistentGraphHead graphData) throws IOException {
    writeLabel(put, graphData);
    writeProperties(put, graphData);
    writeVertices(put, graphData);
    writeEdges(put, graphData);
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public G readGraphHead(final Result res) {
    G graphHead = null;
    try {
      graphHead = graphHeadFactory
        .initGraphHead(readId(res), readLabel(res), readProperties(res));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return graphHead;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EPGMGraphHeadFactory<G> getGraphHeadFactory() {
    return graphHeadFactory;
  }
}
