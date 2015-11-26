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

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.log4j.Logger;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.storage.api.PersistentEdge;
import org.gradoop.util.GConstants;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.storage.api.EdgeHandler;

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
public class HBaseEdgeHandler<ED extends EPGMEdge, VD extends EPGMVertex>
  extends HBaseGraphElementHandler implements EdgeHandler<ED, VD> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * Logger
   */
  private static Logger LOG = Logger.getLogger(HBaseEdgeHandler.class);

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
  private final EPGMEdgeFactory<ED> edgeFactory;

  /**
   * Creates an edge data handler.
   *
   * @param edgeFactory edge data factory
   */
  public HBaseEdgeHandler(EPGMEdgeFactory<ED> edgeFactory) {
    this.edgeFactory = edgeFactory;
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
  public Put writeSourceVertex(Put put, VD vertexData) throws IOException {
    return put.add(CF_META_BYTES, COL_SOURCE_VERTEX_BYTES,
      createVertexIdentifier(vertexData));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId readSourceVertexId(Result res) throws IOException {
    GradoopId sourceVertexId = new GradoopId();
    Writables.getWritable(
      res.getValue(CF_META_BYTES, COL_SOURCE_VERTEX_BYTES), sourceVertexId);

    return sourceVertexId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeTargetVertex(Put put, VD vertexData) throws IOException {
    return put.add(CF_META_BYTES, COL_TARGET_VERTEX_BYTES,
      createVertexIdentifier(vertexData));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId readTargetVertexId(Result res) throws IOException {
    GradoopId targetVertexId = new GradoopId();
    Writables.getWritable(
      res.getValue(CF_META_BYTES, COL_TARGET_VERTEX_BYTES), targetVertexId);

    return targetVertexId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeEdge(Put put, PersistentEdge<VD> edgeData) throws
    IOException {
    LOG.info("Creating Put from: " + edgeData);
    writeLabel(put, edgeData);
    writeSourceVertex(put, edgeData.getSourceVertex());
    writeTargetVertex(put, edgeData.getTargetVertex());
    writeProperties(put, edgeData);
    writeGraphIds(put, edgeData);
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ED readEdge(Result res) {
    ED edge = null;
    try {
      edge = edgeFactory
        .initEdge(readId(res), readLabel(res), readSourceVertexId(res),
          readTargetVertexId(res), readProperties(res), readGraphIds(res));
    } catch (IOException e) {
      e.printStackTrace();
    }

    return edge;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EPGMEdgeFactory<ED> getEdgeFactory() {
    return edgeFactory;
  }

  /**
   * Serializes a vertex to a vertex identifier in the following format:
   *
   * <vertex-identifier> ::= <vertex-id><vertex-label>
   *
   * @param vertex vertex
   * @return byte representation of the vertex identifier
   */
  private byte[] createVertexIdentifier(final EPGMVertex vertex) throws
    IOException {
    byte[] vertexKeyBytes = Writables.getBytes(vertex.getId());
    byte[] labelBytes = Bytes.toBytes(vertex.getLabel());

    ArrayUtils.addAll(vertexKeyBytes, labelBytes);

    return vertexKeyBytes;
  }
}
