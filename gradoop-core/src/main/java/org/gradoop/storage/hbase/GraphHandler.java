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
   * Adds all vertices of the given graph to the given
   * {@link org.apache.hadoop.hbase.client.Put} and returns it.
   *
   * @param put   put to add vertices to
   * @param graph graph whose vertices shall be added
   * @return put with vertices
   */
  Put writeVertices(final Put put, final Graph graph);

  /**
   * Reads the vertex identifiers of a given graph from the given result.
   *
   * @param res HBase row
   * @return vertex identifiers stored in the given result
   */
  Iterable<Long> readVertices(final Result res);
}