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
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.storage.api.EdgeHandler;
import org.gradoop.common.storage.api.PersistentEdge;
import org.gradoop.common.util.GConstants;

import java.io.IOException;

/**
 * Used to read/write EPGM edge data from/to a HBase table.
 * <p>
 * EPGMEdge data in HBase:
 * <p>
 * |---------|---------------------------------------------|-------|
 * | row-key | meta                                        | data  |
 * |---------|----------|------------|------------|--------|-------|
 * | "0"     | label    | source     | target     | graphs | since |
 * |         |----------|------------|------------|--------|-------|
 * |         | "knows"  | <Person.0> | <Person.1> | [0,1]  | 2014  |
 * |---------|----------|------------|------------|--------|-------|
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class HBaseEdgeHandler<E extends EPGMEdge, V extends EPGMVertex>
  extends HBaseGraphElementHandler
  implements EdgeHandler<E, V> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * Byte array representation of the source vertex column identifier.
   */
  private static final byte[] COL_SOURCE_BYTES =
    Bytes.toBytes(GConstants.COL_SOURCE);
  /**
   * Byte array representation of the target vertex column identifier.
   */
  private static final byte[] COL_TARGET_BYTES =
    Bytes.toBytes(GConstants.COL_TARGET);

  /**
   * Creates edge data objects from the rows.
   */
  private final EPGMEdgeFactory<E> edgeFactory;

  /**
   * Creates an edge data handler.
   *
   * @param edgeFactory edge data factory
   */
  public HBaseEdgeHandler(EPGMEdgeFactory<E> edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createTable(final HBaseAdmin admin,
    final HTableDescriptor tableDescriptor) throws IOException {
    tableDescriptor.addFamily(new HColumnDescriptor(GConstants.CF_META));
    tableDescriptor.addFamily(new HColumnDescriptor(GConstants.CF_PROPERTIES));
    admin.createTable(tableDescriptor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeSource(Put put, V vertexData) throws IOException {
    return put.add(CF_META_BYTES, COL_SOURCE_BYTES,
      createVertexIdentifier(vertexData));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId readSourceId(Result res) throws IOException {
    GradoopId sourceVertexId = new GradoopId();
    Writables.getWritable(
      res.getValue(CF_META_BYTES, COL_SOURCE_BYTES), sourceVertexId);

    return sourceVertexId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeTarget(Put put, V vertexData) throws IOException {
    return put.add(CF_META_BYTES, COL_TARGET_BYTES,
      createVertexIdentifier(vertexData));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId readTargetId(Result res) throws IOException {
    GradoopId targetVertexId = new GradoopId();
    Writables.getWritable(
      res.getValue(CF_META_BYTES, COL_TARGET_BYTES), targetVertexId);

    return targetVertexId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeEdge(Put put, PersistentEdge<V> edgeData) throws
    IOException {
    writeLabel(put, edgeData);
    writeSource(put, edgeData.getSource());
    writeTarget(put, edgeData.getTarget());
    writeProperties(put, edgeData);
    writeGraphIds(put, edgeData);
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public E readEdge(Result res) {
    E edge = null;
    try {
      edge = edgeFactory
        .initEdge(readId(res), readLabel(res), readSourceId(res),
          readTargetId(res), readProperties(res), readGraphIds(res));
    } catch (IOException e) {
      e.printStackTrace();
    }

    return edge;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EPGMEdgeFactory<E> getEdgeFactory() {
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
  private byte[] createVertexIdentifier(final V vertex) throws
    IOException {
    byte[] vertexKeyBytes = Writables.getBytes(vertex.getId());
    byte[] labelBytes = Bytes.toBytes(vertex.getLabel());

    ArrayUtils.addAll(vertexKeyBytes, labelBytes);

    return vertexKeyBytes;
  }
}
