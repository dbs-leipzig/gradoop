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

package org.gradoop.common.storage.api;

import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

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
  GradoopIdSet getVertexIds();

  /**
   * Sets the vertices that are contained in that graph.
   *
   * @param vertices vertex ids
   */
  void setVertexIds(GradoopIdSet vertices);

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
  GradoopIdSet getEdgeIds();

  /**
   * Sets the edges that are contained in that graph.
   *
   * @param edges edge ids
   */
  void setEdgeIds(GradoopIdSet edges);

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
