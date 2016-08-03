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

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.storage.api.PersistentVertex;
import org.gradoop.common.storage.api.VertexHandler;
import org.gradoop.common.util.GConstants;

import java.io.IOException;
import java.util.Set;

/**
 * Used to read/write EPGM vertex data from/to a HBase table.
 * <p>
 * EPGMVertex data in HBase:
 * <p>
 * |---------|--------------------|---------|-------------|-------------|
 * | row-key | meta               | data    | out-edges   | in-edges    |
 * |---------|----------|---------|---------|-------------|-------------|
 * | "0"     | label    | graphs  | k1 | k2 | <0.1.knows> | <1.0.knows> |
 * |         |----------|---------|----|----|-------------|-------------|
 * |         | "Person" |  [0,2]  | v1 | v2 |             |             |
 * |---------|----------|---------|----|----|-------------|-------------|
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class HBaseVertexHandler<V extends EPGMVertex, E extends EPGMEdge>
  extends HBaseGraphElementHandler implements VertexHandler<V, E> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

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
  private final EPGMVertexFactory<V> vertexFactory;

  /**
   * Creates a vertex handler.
   *
   * @param vertexFactory used to create runtime vertex data objects
   */
  public HBaseVertexHandler(EPGMVertexFactory<V> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createTable(final HBaseAdmin admin,
    final HTableDescriptor tableDescriptor) throws IOException {
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
  public Put writeOutgoingEdges(final Put put, final Set<E> outgoingEdgeData)
      throws IOException {
    return writeEdges(put, CF_OUT_EDGES_BYTES, outgoingEdgeData, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeIncomingEdges(final Put put, final Set<E> incomingEdgeData)
      throws IOException {
    return writeEdges(put, CF_IN_EDGES_BYTES, incomingEdgeData, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeVertex(
    final Put put, final PersistentVertex<E> vertexData) throws IOException {
    writeLabel(put, vertexData);
    writeProperties(put, vertexData);
    writeOutgoingEdges(put, vertexData.getOutgoingEdges());
    writeIncomingEdges(put, vertexData.getIncomingEdges());
    writeGraphIds(put, vertexData);
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
  public V readVertex(final Result res) {
    V vertex = null;
    try {
      vertex = vertexFactory.initVertex(
        readId(res), readLabel(res), readProperties(res), readGraphIds(res));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return vertex;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EPGMVertexFactory<V> getVertexFactory() {
    return vertexFactory;
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
    final Set<E> edgeDataSet, boolean isOutgoing) throws IOException {
    if (edgeDataSet != null) {
      for (E edge : edgeDataSet) {
        put = writeEdge(put, columnFamily, edge, isOutgoing);
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
   * @param edge     edge to store
   * @param isOutgoing   true, if the edge is an outgoing edge, false if
   *                     incoming
   * @return the updated put
   */
  private Put writeEdge(final Put put, final byte[] columnFamily,
    final E edge, boolean isOutgoing) throws IOException {
    byte[] edgeKey = createEdgeIdentifier(edge, isOutgoing);
    put.add(columnFamily, edgeKey, null);
    return put;
  }

  /**
   * Serializes an edge to an edge identifier in the following format:
   * <p>
   * <edge-identifier> ::= <edgeId><otherID><label>
   *
   * @param edge   edge to create identifier for
   * @param isOutgoing true, if the edge is an outgoing edge, false if
   *                   incoming
   * @return byte representation of the edge identifier
   */
  private byte[] createEdgeIdentifier(final E edge, boolean isOutgoing)
      throws IOException {

    // initially only GradoopId
    byte[] edgeIdentifier = Writables.getBytes(edge.getId());

    // extend by source or vertex id
    byte[] otherVertexIdBytes = Writables.getBytes(
      isOutgoing ? edge.getTargetId() : edge.getSourceId());
    ArrayUtils.addAll(edgeIdentifier, otherVertexIdBytes);

    // extend by label
    byte[] labelBytes = Bytes.toBytes(edge.getLabel());
    ArrayUtils.addAll(edgeIdentifier, labelBytes);

    return edgeIdentifier;
  }
}
