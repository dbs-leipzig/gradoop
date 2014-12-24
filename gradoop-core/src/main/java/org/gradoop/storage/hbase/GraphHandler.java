package org.gradoop.storage.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.model.Graph;

/**
 * VertexHandler is responsible for reading and writing EPG graphs from and to
 * HBase.
 */
public interface GraphHandler extends EntityHandler {

  /**
   * Creates a globally unique row key based on the given vertexID. The
   * created row key is used to persist the vertex in the graph store.
   *
   * @param graphID graphID to create row key from (must not be {@code null}).
   * @return persistent graph identifier
   */
  byte[] getRowKey(final Long graphID);

  /**
   * Creates a graph identifier from a given row key.
   *
   * @param rowKey row key from the graph store (must not be {@code null})
   * @return transient graph identifier
   */
  Long getGraphID(final byte[] rowKey);

  /**
   * Adds all vertices of the given graph to the given
   * {@link org.apache.hadoop.hbase.client.Put} and returns it.
   *
   * @param put   put to add vertices to
   * @param graph graph whose vertices shall be added
   * @return put with vertices
   */
  Put writeVertices(final Put put, final Graph graph);

  /**
   * Adds all graph information to the given
   * {@link org.apache.hadoop.hbase.client.Put} and returns it.
   *
   * @param put   put to add graph to
   * @param graph graph whose information shall be added
   * @return put with graph information
   */
  Put writeGraph(final Put put, final Graph graph);

  /**
   * Reads the vertex identifiers of a given graph from the given result.
   *
   * @param res HBase row
   * @return vertex identifiers stored in the given result
   */
  Iterable<Long> readVertices(final Result res);

  /**
   * Reads the complete graph from the given result.
   *
   * @param res HBase row
   * @return graph entity
   */
  Graph readGraph(final Result res);
}
