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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.storage.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.model.GraphData;
import org.gradoop.model.GraphDataFactory;
import org.gradoop.storage.GraphDataHandler;
import org.gradoop.storage.PersistentGraphData;

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
 * @param <GD> graph data type
 */
public class DefaultGraphDataHandler<GD extends GraphData> extends
  DefaultElementHandler implements GraphDataHandler<GD> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * Class logger.
   */
  private static Logger LOG = Logger.getLogger(DefaultGraphDataHandler.class);

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
  private final GraphDataFactory<GD> graphDataFactory;

  /**
   * Creates a graph handler.
   *
   * @param graphDataFactory used to create runtime graph data objects
   */
  public DefaultGraphDataHandler(GraphDataFactory<GD> graphDataFactory) {
    this.graphDataFactory = graphDataFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createTable(final HBaseAdmin admin,
    final HTableDescriptor tableDescriptor) throws IOException {
    LOG.info("Creating table " + tableDescriptor.getNameAsString());
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
  public Put writeVertices(final Put put, final PersistentGraphData graphData) {
    for (Long vertexId : graphData.getVertices()) {
      put.add(CF_VERTICES_BYTES, Bytes.toBytes(vertexId), null);
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
  public Put writeEdges(Put put, PersistentGraphData graphData) {
    for (Long edgeId : graphData.getEdges()) {
      put.add(CF_EDGES_BYTES, Bytes.toBytes(edgeId), null);
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
  public Put writeGraphData(final Put put,
    final PersistentGraphData graphData) {
    LOG.info("Creating Put from: " + graphData);
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
  public GD readGraphData(final Result res) {
    return graphDataFactory
      .createGraphData(Long.valueOf(Bytes.toString(res.getRow())),
        readLabel(res), readProperties(res));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphDataFactory<GD> getGraphDataFactory() {
    return graphDataFactory;
  }
}
