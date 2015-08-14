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
import org.gradoop.model.EdgeData;
import org.gradoop.model.EdgeDataFactory;
import org.gradoop.model.VertexData;
import org.gradoop.storage.EdgeDataHandler;
import org.gradoop.storage.PersistentEdgeData;

import java.io.IOException;

/**
 * Used to read/write EPGM edge data from/to a HBase table.
 * <p>
 * Edge data in HBase:
 * <p>
 * |---------|---------------------------------------------|-------|
 * | row-key | meta                                        | data  |
 * |---------|----------|------------|------------|--------|-------|
 * | "0"     | label    | source     | target     | graphs | since |
 * |         |----------|------------|------------|--------|-------|
 * |         | "knows"  | <Person.0> | <Person.1> | [0,1]  | 2014  |
 * |---------|----------|------------|------------|--------|-------|
 *
 * @param <ED> edge data type
 * @param <VD> vertex data type (used to create persistent vertex identifiers)
 */
public class DefaultEdgeDataHandler<ED extends EdgeData, VD extends
  VertexData> extends
  DefaultGraphElementHandler implements EdgeDataHandler<ED, VD> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * Logger
   */
  private static Logger LOG = Logger.getLogger(DefaultEdgeDataHandler.class);

  /**
   * Byte array representation of the source vertex column identifier.
   */
  private static final byte[] COL_SOURCE_VERTEX_BYTES =
    Bytes.toBytes(GConstants.COL_SOURCE_VERTEX);
  /**
   * Byte array representation of the target vertex column identifier.
   */
  private static final byte[] COL_TARGET_VERTEX_BYTES =
    Bytes.toBytes(GConstants.COL_TARGET_VERTEX);

  /**
   * Creates edge data objects from the rows.
   */
  private final EdgeDataFactory<ED> edgeDataFactory;

  /**
   * Creates an edge data handler.
   *
   * @param edgeDataFactory edge data factory
   */
  public DefaultEdgeDataHandler(EdgeDataFactory<ED> edgeDataFactory) {
    this.edgeDataFactory = edgeDataFactory;
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
    admin.createTable(tableDescriptor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeSourceVertex(Put put, VD vertexData) {
    return put.add(CF_META_BYTES, COL_SOURCE_VERTEX_BYTES,
      createVertexIdentifier(vertexData));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Long readSourceVertexId(Result res) {
    return Bytes.toLong(res.getValue(CF_META_BYTES, COL_SOURCE_VERTEX_BYTES));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeTargetVertex(Put put, VD vertexData) {
    return put.add(CF_META_BYTES, COL_TARGET_VERTEX_BYTES,
      createVertexIdentifier(vertexData));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Long readTargetVertexId(Result res) {
    return Bytes.toLong(res.getValue(CF_META_BYTES, COL_TARGET_VERTEX_BYTES));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeEdgeData(Put put, PersistentEdgeData<VD> edgeData) {
    LOG.info("Creating Put from: " + edgeData);
    writeLabel(put, edgeData);
    writeSourceVertex(put, edgeData.getSourceVertexData());
    writeTargetVertex(put, edgeData.getTargetVertexData());
    writeProperties(put, edgeData);
    writeGraphs(put, edgeData);
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ED readEdgeData(Result res) {
    return edgeDataFactory
      .createEdgeData(Long.valueOf(Bytes.toString(res.getRow())),
        readLabel(res), readSourceVertexId(res), readTargetVertexId(res),
        readProperties(res), readGraphs(res));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EdgeDataFactory<ED> getEdgeDataFactory() {
    return edgeDataFactory;
  }

  /**
   * Serializes a vertex to a vertex identifier in the following format:
   *
   * <vertex-identifier> ::= <vertex-id><vertex-label>
   *
   * @param vertexData vertexData
   * @return byte representation of the vertex identifier
   */
  private byte[] createVertexIdentifier(final VertexData vertexData) {
    byte[] labelBytes = Bytes.toBytes(vertexData.getLabel());
    byte[] vertexKey = new byte[Bytes.SIZEOF_LONG + labelBytes.length];
    // vertex identifier
    Bytes.putLong(vertexKey, 0, vertexData.getId());
    // vertex label
    Bytes
      .putBytes(vertexKey, Bytes.SIZEOF_LONG, labelBytes, 0, labelBytes.length);
    return vertexKey;
  }
}
