package org.gradoop.storage;

import org.gradoop.model.Graph;
import org.gradoop.model.Vertex;

/**
 * A graph store is responsible for writing and reading graphs including
 * vertices and edges.
 */
public interface GraphStore {
  /**
   * Writes the given graph into the graph store.
   *
   * @param graph graph to write
   */
  void writeGraph(final Graph graph);

  /**
   * Writes the given vertex into the graph store.
   *
   * @param vertex vertex to write
   */
  void writeVertex(final Vertex vertex);

  /**
   * Reads a graph entity from the graph store using the given graph id. If
   * {@code graphID} does not exist, {@code null} is returned.
   *
   * @param graphID graph id
   * @return graph entity or {@code null} if there is no entity with the given
   * {@code graphId}
   */
  Graph readGraph(final Long graphID);

  /**
   * Reads a vertex entity from the graph store using the given vertex id. If
   * {@code vertexID} does not exist, {@code null} is returned.
   *
   * @param vertexID vertex id
   * @return vertex entity or {@code null} if there is no entity with the given
   * {@code vertexID}
   */
  Vertex readVertex(final Long vertexID);

  /**
   * Setting this value to true, forces the store implementation to flush the
   * write buffers after every write.
   *
   * @param autoFlush true to enable auto flush, false to disable
   */
  void setAutoFlush(boolean autoFlush);

  /**
   * Flushes all buffered writes to the store.
   */
  void flush();

  /**
   * Closes the graph store and flushes all writes.
   */
  void close();
}
