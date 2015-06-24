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

package org.gradoop.model;

/**
 * A graph consists of a set of vertices.
 * <p/>
 * Graphs can overlap, in that case, in that case, vertices inside the overlap
 * are assigned to more than one graph. Edges are either part of one graph
 * (intra-edges) or connect vertices in different graphs (inter-edges).
 * <p/>
 * In the case of intra-edges, both the source as well as the target vertex are
 * attached to the same graph. Looking at inter-edges, source and target vertex
 * are attached to different graphs. If multiple graphs overlap, an edge can be
 * inter- and intra-edge at the same time.
 */
public interface Graph extends Identifiable, Attributed, Labeled {
  /**
   * Adds the given vertex identifier to the graph.
   *
   * @param vertexID vertex identifier
   */
  void addVertex(Long vertexID);

  /**
   * Returns all vertices contained in that graph.
   *
   * @return vertices
   */
  Iterable<Long> getVertices();

  /**
   * Returns the number of vertices belonging to that graph.
   *
   * @return vertex count
   */
  int getVertexCount();
}
