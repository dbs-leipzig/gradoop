
package org.gradoop.common.storage.api;

import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Representation of vertex data on the storage level. We additionally store
 * vertices and edges contained in the graph for faster graph retrieval.
 */
public interface PersistentGraphHead extends EPGMGraphHead {
  /**
   * Returns all vertex identifiers that are contained in that graph.
   *
   * @return vertex ids that are contained in that graph
   */
  GradoopIdList getVertexIds();

  /**
   * Sets the vertices that are contained in that graph.
   *
   * @param vertices vertex ids
   */
  void setVertexIds(GradoopIdList vertices);

  /**
   * Adds a vertex identifier to the graph data.
   *
   * @param vertex vertex id
   */
  void addVertexId(GradoopId vertex);

  /**
   * Returns the number of vertices stored in the graph data.
   *
   * @return number of vertices
   */
  long getVertexCount();

  /**
   * Returns all edge identifiers that are contained in that graph.
   *
   * @return edge ids that are contained in that graph
   */
  GradoopIdList getEdgeIds();

  /**
   * Sets the edges that are contained in that graph.
   *
   * @param edges edge ids
   */
  void setEdgeIds(GradoopIdList edges);

  /**
   * Adds an edge identifier to the graph data.
   *
   * @param edge edge id
   */
  void addEdgeId(GradoopId edge);

  /**
   * Returns the number of edges stored in the graph data.
   *
   * @return edge count
   */
  long getEdgeCount();
}
