package org.gradoop.storage.hbase;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.model.Edge;
import org.gradoop.model.GraphElement;
import org.gradoop.model.Vertex;

import java.io.IOException;

/**
 * VertexHandler is responsible for reading and writing EPG vertices from and to
 * HBase.
 */
public interface VertexHandler extends EntityHandler {
  /**
   * Creates vertices table based on the given table descriptor.
   *
   * @param admin           HBase admin
   * @param tableDescriptor description of the vertices table used by that
   *                        handler
   * @throws IOException
   */
  void createVerticesTable(final HBaseAdmin admin,
                           final HTableDescriptor tableDescriptor)
    throws IOException;

  /**
   * Creates a globally unique row key based on the given vertexID. The
   * created row key is used to persist the vertex in the graph store.
   *
   * @param vertexID vertexID to create row key from (must not be {@code null}).
   * @return persistent vertex identifier
   */
  byte[] getRowKey(final Long vertexID);

  /**
   * Creates a vertex identifier from a given row key.
   *
   * @param rowKey row key from the graph store (must not be {@code null})
   * @return transient vertex identifier
   */
  Long getVertexID(final byte[] rowKey);

  /**
   * Adds the given outgoing edges to the given
   * {@link org.apache.hadoop.hbase.client.Put}
   * and returns it.
   *
   * @param put   {@link org.apache.hadoop.hbase.client.Put} to add edges to
   * @param edges edges to add
   * @return put with edges
   */
  Put writeOutgoingEdges(final Put put, final Iterable<? extends Edge> edges);

  /**
   * Adds the given incoming edges to the given
   * {@link org.apache.hadoop.hbase.client.Put}
   * and returns it.
   *
   * @param put   {@link org.apache.hadoop.hbase.client.Put} to add edges to
   * @param edges edges to add
   * @return put with edges
   */
  Put writeIncomingEdges(final Put put, final Iterable<? extends Edge> edges);

  /**
   * Adds the given graphs information to the given
   * {@link org.apache.hadoop.hbase.client.Put} and returns it.
   *
   * @param put          {@link org.apache.hadoop.hbase.client.Put} to add
   *                     graphs to
   * @param graphElement element contained in graphs
   * @return put with graph information
   */
  Put writeGraphs(final Put put, final GraphElement graphElement);

  /**
   * Writes the complete vertex information to the given
   * {@link org.apache.hadoop.hbase.client.Put} and returns it.
   *
   * @param put    {@link org.apache.hadoop.hbase.client.Put} to add vertex to
   * @param vertex vertex to be written
   * @return put with vertex information
   */
  Put writeVertex(final Put put, final Vertex vertex);

  /**
   * Reads the outgoing edges from the given
   * {@link org.apache.hadoop.hbase.client.Result}.
   *
   * @param res HBase row
   * @return outgoing edges contained in the given result
   */
  Iterable<Edge> readOutgoingEdges(final Result res);

  /**
   * Reads the incoming edges from the given
   * {@link org.apache.hadoop.hbase.client.Result}.
   *
   * @param res HBase row
   * @return incoming edges contained in the given result
   */
  Iterable<Edge> readIncomingEdges(final Result res);

  /**
   * Reads the graphs from the given
   * {@link org.apache.hadoop.hbase.client.Result}.
   *
   * @param res HBase row
   * @return graphs contained in the given result
   */
  Iterable<Long> readGraphs(final Result res);

  /**
   * Reads the complete vertex from the given
   * {@link org.apache.hadoop.hbase.client.Result}.
   *
   * @param res HBase row
   * @return vertex contained in the given result.
   */
  Vertex readVertex(final Result res);
}
