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

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.storage.api.EdgeHandler;
import org.gradoop.common.storage.api.PersistentEdge;
import org.gradoop.common.util.HBaseConstants;

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
  private static final byte[] COL_SOURCE_BYTES = Bytes.toBytes(HBaseConstants.COL_SOURCE);
  /**
   * Byte array representation of the target vertex column identifier.
   */
  private static final byte[] COL_TARGET_BYTES = Bytes.toBytes(HBaseConstants.COL_TARGET);

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
  public void createTable(final HBaseAdmin admin, final HTableDescriptor tableDescriptor)
    throws IOException {
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseConstants.CF_META));
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseConstants.CF_PROPERTIES));
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
    return GradoopId.fromByteArray(res.getValue(CF_META_BYTES, COL_SOURCE_BYTES));
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
    return GradoopId.fromByteArray(res.getValue(CF_META_BYTES, COL_TARGET_BYTES));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeEdge(Put put, PersistentEdge<V> edgeData) throws IOException {
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
  private byte[] createVertexIdentifier(final V vertex) throws IOException {
    byte[] vertexKeyBytes = vertex.getId().toByteArray();
    byte[] labelBytes = Bytes.toBytes(vertex.getLabel());

    ArrayUtils.addAll(vertexKeyBytes, labelBytes);

    return vertexKeyBytes;
  }
}
