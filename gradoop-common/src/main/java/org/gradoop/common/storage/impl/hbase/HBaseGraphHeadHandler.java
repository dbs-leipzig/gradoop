/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.common.storage.impl.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.storage.api.GraphHeadHandler;
import org.gradoop.common.storage.api.PersistentGraphHead;
import org.gradoop.common.util.GConstants;
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
  private static final byte[] CF_VERTICES_BYTES =
    Bytes.toBytes(GConstants.CF_VERTICES);

  /**
   * Byte array representation of the edges column family.
   */
  private static final byte[] CF_EDGES_BYTES =
    Bytes.toBytes(GConstants.CF_EDGES);

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
  public void createTable(final HBaseAdmin admin,
    final HTableDescriptor tableDescriptor) throws IOException {
    tableDescriptor.addFamily(new HColumnDescriptor(GConstants.CF_META));
    tableDescriptor.addFamily(new HColumnDescriptor(GConstants.CF_PROPERTIES));
    tableDescriptor.addFamily(new HColumnDescriptor(GConstants.CF_VERTICES));
    tableDescriptor.addFamily(new HColumnDescriptor(GConstants.CF_EDGES));
    admin.createTable(tableDescriptor);
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeVertices(
    final Put put, final PersistentGraphHead graphData) throws IOException {

    for (GradoopId vertexId : graphData.getVertexIds()) {
      put.add(CF_VERTICES_BYTES, Writables.getBytes(vertexId), null);
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
  public Put writeEdges(Put put, PersistentGraphHead graphData) throws
    IOException {
    for (GradoopId edgeId : graphData.getEdgeIds()) {
      put.add(CF_EDGES_BYTES, Writables.getBytes(edgeId), null);
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
  public Put writeGraphHead(final Put put, final PersistentGraphHead
    graphData) throws IOException {
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
