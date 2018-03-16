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
