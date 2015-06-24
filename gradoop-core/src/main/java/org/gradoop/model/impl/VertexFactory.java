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

package org.gradoop.model.impl;

import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;

import java.util.Map;

/**
 * Factory for creating vertices.
 */
public class VertexFactory {

  /**
   * Avoid instantiation.
   */
  private VertexFactory() {
  }

  /**
   * Creates a vertex using the given identifier.
   *
   * @param vertexID vertex identifier
   * @return vertex with identifier
   */
  public static Vertex createDefaultVertexWithID(final Long vertexID) {
    return createDefaultVertex(vertexID, null, null, null, null, null);
  }

  /**
   * Creates a vertex with outgoing edges.
   *
   * @param vertexID      vertex identifier
   * @param outgoingEdges edges starting at that vertex
   * @return vertex with identifier and outgoing edges
   */
  public static Vertex createDefaultVertexWithOutgoingEdges(final Long vertexID,
    final Iterable<Edge> outgoingEdges) {
    return createDefaultVertexWithEdges(vertexID, outgoingEdges, null);
  }

  /**
   * Creates a vertex with outgoing and incoming edges.
   *
   * @param vertexID      vertex identifier
   * @param outgoingEdges edges starting at that vertex
   * @param incomingEdges edges ending in that vertex
   * @return vertex with identifier, outgoing and incoming edges
   */
  public static Vertex createDefaultVertexWithEdges(final Long vertexID,
    final Iterable<Edge> outgoingEdges, final Iterable<Edge> incomingEdges) {
    return createDefaultVertex(vertexID, null, null,
      outgoingEdges, incomingEdges, null);
  }

  /**
   * Creates a vertex with labels and outgoing edges.
   *
   * @param vertexID      vertex identifier
   * @param label         vertex labels
   * @param outgoingEdges edges starting at that vertex
   * @return vertex with identifier, labels and outgoing edges
   */
  public static Vertex createDefaultVertexWithLabel(final Long vertexID,
    final String label, final Iterable<Edge> outgoingEdges) {
    return createDefaultVertex(vertexID, label, null, outgoingEdges, null,
      null);
  }

  /**
   * Create a vertex with properties and outgoing edges.
   *
   * @param vertexID      vertex identifier
   * @param properties    vertex properties
   * @param outgoingEdges edges starting at that vertex
   * @return vertex with identifier, properties and outgoing edges
   */
  public static Vertex createDefaultVertexWithProperties(final Long vertexID,
    final Map<String, Object> properties, final Iterable<Edge> outgoingEdges) {
    return createDefaultVertex(vertexID, null,
      properties, outgoingEdges, null, null);
  }

  /**
   * Creates a vertex based on the given properties.
   *
   * @param id            vertex identifier
   * @param label         vertex labels
   * @param properties    vertex properties
   * @param outgoingEdges edges starting at that vertex
   * @param incomingEdges edges ending in that vertex
   * @param graphs        graphs that vertex belongs to
   * @return vertex
   */
  public static Vertex createDefaultVertex(final Long id, final String label,
    final Map<String, Object> properties, final Iterable<Edge> outgoingEdges,
    final Iterable<Edge> incomingEdges, final Iterable<Long> graphs) {
    checkVertexID(id);

    return new DefaultVertex(id, label, properties, outgoingEdges,
      incomingEdges, graphs);
  }

  /**
   * Checks if the given vertexID is valid.
   *
   * @param vertexID vertex identifier
   */
  private static void checkVertexID(final Long vertexID) {
    if (vertexID == null) {
      throw new IllegalArgumentException("vertexID must not be null");
    }
  }
}
