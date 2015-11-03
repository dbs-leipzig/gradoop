/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.api.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;

/**
 * Operators that are available at all graph structures.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @see LogicalGraph
 * @see org.gradoop.model.impl.GraphCollection
 */
public interface GraphOperators<
  VD extends EPGMVertex,
  ED extends EPGMEdge> {

  /**
   * Returns all vertices including vertex data associated with that graph.
   *
   * @return vertices
   */
  DataSet<VD> getVertices();

  /**
   * Returns all edge data associated with that logical graph.
   *
   * @return edges
   */
  DataSet<ED> getEdges();

  /**
   * Returns the edge data associated with the outgoing edges of the given
   * vertex.
   *
   * @param vertexID vertex identifier
   * @return outgoing edge data of given vertex
   */
  DataSet<ED> getOutgoingEdges(final Long vertexID);

  /**
   * Returns the edge data associated with the incoming edges of the given
   * vertex.
   *
   * @param vertexID vertex identifier
   * @return incoming edge data of given vertex
   */
  DataSet<ED> getIncomingEdges(final Long vertexID);

  /**
   * Transforms the EPGM graph to a Gelly Graph.
   *
   * @return Gelly Graph
   */
  Graph<Long, VD, ED> toGellyGraph();

  /**
   * Returns the number of vertices in that logical graph.
   *
   * @return number of vertices
   * @throws Exception
   */
  long getVertexCount() throws Exception;

  /**
   * Returns the number of edges in that logical graph.
   *
   * @return number of edges
   * @throws Exception
   */
  long getEdgeCount() throws Exception;
}
