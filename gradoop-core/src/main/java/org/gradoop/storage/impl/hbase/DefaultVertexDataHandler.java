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

package org.gradoop.storage.impl.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.gradoop.util.GConstants;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.api.VertexDataFactory;
import org.gradoop.storage.api.PersistentVertexData;
import org.gradoop.storage.api.VertexDataHandler;

import java.io.IOException;
import java.util.Set;

/**
 * Used to read/write EPGM vertex data from/to a HBase table.
 * <p>
 * Vertex data in HBase:
 * <p>
 * |---------|--------------------|---------|-------------|-------------|
 * | row-key | meta               | data    | out-edges   | in-edges    |
 * |---------|----------|---------|---------|-------------|-------------|
 * | "0"     | label    | graphs  | k1 | k2 | <0.1.knows> | <1.0.knows> |
 * |         |----------|---------|----|----|-------------|-------------|
 * |         | "Person" |  [0,2]  | v1 | v2 |             |             |
 * |---------|----------|---------|----|----|-------------|-------------|
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 */
public class DefaultVertexDataHandler<VD extends VertexData, ED extends
  EdgeData> extends
  DefaultGraphElementHandler implements VertexDataHandler<VD, ED> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * Logger
   */
  private static Logger LOG = Logger.getLogger(DefaultVertexDataHandler.class);

  /**
   * Byte array representation of the outgoing edges column family.
   */
  private static final byte[] CF_OUT_EDGES_BYTES =
    Bytes.toBytes(GConstants.CF_OUT_EDGES);
  /**
   * Byte array representation of the incoming edges column family.
   */
  private static final byte[] CF_IN_EDGES_BYTES =
    Bytes.toBytes(GConstants.CF_IN_EDGES);

  /**
   * Creates vertex data objects from the rows.
   */
  private final VertexDataFactory<VD> vertexDataFactory;

  /**
   * Creates a vertex handler.
   *
   * @param vertexDataFactory used to create runtime vertex data objects
   */
  public DefaultVertexDataHandler(VertexDataFactory<VD> vertexDataFactory) {
    this.vertexDataFactory = vertexDataFactory;
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
    tableDescriptor.addFamily(new HColumnDescriptor(GConstants.CF_OUT_EDGES));
    tableDescriptor.addFamily(new HColumnDescriptor(GConstants.CF_IN_EDGES));
    admin.createTable(tableDescriptor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeOutgoingEdges(final Put put, final Set<ED> outgoingEdgeData) {
    return writeEdges(put, CF_OUT_EDGES_BYTES, outgoingEdgeData, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeIncomingEdges(final Put put, final Set<ED> incomingEdgeData) {
    return writeEdges(put, CF_IN_EDGES_BYTES, incomingEdgeData, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeVertexData(final Put put,
    final PersistentVertexData<ED> vertexData) {
    LOG.info("Creating Put from: " + vertexData);
    writeLabel(put, vertexData);
    writeProperties(put, vertexData);
    writeOutgoingEdges(put, vertexData.getOutgoingEdgeData());
    writeIncomingEdges(put, vertexData.getIncomingEdgeData());
    writeGraphs(put, vertexData);
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<Long> readOutgoingEdgeIds(final Result res) {
    return getColumnKeysFromFamily(res, CF_OUT_EDGES_BYTES);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<Long> readIncomingEdgeIds(final Result res) {
    return getColumnKeysFromFamily(res, CF_IN_EDGES_BYTES);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VD readVertexData(final Result res) {
    return vertexDataFactory
      .createVertexData(Long.valueOf(Bytes.toString(res.getRow())),
        readLabel(res), readProperties(res), readGraphs(res));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexDataFactory<VD> getVertexDataFactory() {
    return vertexDataFactory;
  }

  /**
   * Adds edgeDataSet to the the given HBase put.
   *
   * @param put          {@link org.apache.hadoop.hbase.client.Put} to
   *                     write the
   *                     edgeDataSet to
   * @param columnFamily CF where the edgeDataSet shall be stored
   * @param edgeDataSet  edgeDataSet to store
   * @param isOutgoing   true, if the edge is an outgoing edge, false if
   *                     incoming
   * @return the updated put
   */
  private Put writeEdges(Put put, final byte[] columnFamily,
    final Set<ED> edgeDataSet, boolean isOutgoing) {
    if (edgeDataSet != null) {
      for (EdgeData edgeData : edgeDataSet) {
        put = writeEdge(put, columnFamily, edgeData, isOutgoing);
      }
    }
    return put;
  }

  /**
   * Writes a single edge to a given put.
   *
   * @param put          {@link org.apache.hadoop.hbase.client.Put} to
   *                     write the
   *                     edge to
   * @param columnFamily CF where the edges shall be stored
   * @param edgeData     edge to store
   * @param isOutgoing   true, if the edge is an outgoing edge, false if
   *                     incoming
   * @return the updated put
   */
  private Put writeEdge(final Put put, final byte[] columnFamily,
    final EdgeData edgeData, boolean isOutgoing) {
    byte[] edgeKey = createEdgeIdentifier(edgeData, isOutgoing);
    put.add(columnFamily, edgeKey, null);
    return put;
  }

  /**
   * Serializes an edge to an edge identifier in the following format:
   * <p>
   * <edge-identifier> ::= <edgeId><otherID><label>
   *
   * @param edgeData   edge to create identifier for
   * @param isOutgoing true, if the edge is an outgoing edge, false if
   *                   incoming
   * @return byte representation of the edge identifier
   */
  private byte[] createEdgeIdentifier(final EdgeData edgeData,
    boolean isOutgoing) {
    byte[] labelBytes = Bytes.toBytes(edgeData.getLabel());
    byte[] edgeKey = new byte[2 * Bytes.SIZEOF_LONG + labelBytes.length];
    // edge identifier
    Bytes.putLong(edgeKey, 0, edgeData.getId());
    // source|target vertex identifier
    Bytes.putLong(edgeKey, Bytes.SIZEOF_LONG,
      isOutgoing ? edgeData.getTargetVertexId() : edgeData.getSourceVertexId());
    // edge label
    Bytes.putBytes(edgeKey, Bytes.SIZEOF_LONG * 2, labelBytes, 0,
      labelBytes.length);
    return edgeKey;
  }
}
