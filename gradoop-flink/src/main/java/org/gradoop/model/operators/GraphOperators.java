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

package org.gradoop.model.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.EdgeData;
import org.gradoop.model.VertexData;

/**
 * Operators that are available at all graph structures.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @see org.gradoop.model.impl.LogicalGraph
 * @see org.gradoop.model.impl.GraphCollection
 */
public interface GraphOperators<
  VD extends VertexData,
  ED extends EdgeData> {

  /**
   * Returns all vertices including vertex data associated with that graph.
   *
   * @return vertices
   */
  DataSet<Vertex<Long, VD>> getVertices();

  /**
   * Returns all vertex data associated with that graph.
   *
   * @return vertex data
   */
  DataSet<VD> getVertexData();

  /**
   * Returns all edge data associated with that logical graph.
   *
   * @return edges
   */
  DataSet<Edge<Long, ED>> getEdges();

  /**
   * Returns all edge data associated with that logical graph.
   *
   * @return edge data
   */
  DataSet<ED> getEdgeData();

  /**
   * Returns the edge data associated with the outgoing edges of the given
   * vertex.
   *
   * @param vertexID vertex identifier
   * @return outgoing edge data of given vertex
   */
  DataSet<Edge<Long, ED>> getOutgoingEdges(final Long vertexID);

  /**
   * Returns the edge data associated with the incoming edges of the given
   * vertex.
   *
   * @param vertexID vertex identifier
   * @return incoming edge data of given vertex
   */
  DataSet<Edge<Long, ED>> getIncomingEdges(final Long vertexID);

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
