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

import org.gradoop.GConstants;
import org.gradoop.model.VertexData;
import scala.collection.generic.ParFactory;

import java.util.Map;
import java.util.Set;

/**
 * Factory for creating vertices.
 */
public class VertexDataFactory {

  /**
   * Avoid instantiation.
   */
  private VertexDataFactory() {
  }

  /**
   * Creates a vertex using the given identifier.
   *
   * @param vertexID vertex identifier
   * @return vertex with identifier
   */
  public static VertexData createDefaultVertexWithID(final Long vertexID) {
    return createDefaultVertex(vertexID, GConstants.DEFAULT_VERTEX_LABEL, null,
      null);
  }

  /**
   * Creates a vertex with outgoing and incoming edges.
   *
   * @param vertexID vertex identifier
   * @return vertex with identifier, outgoing and incoming edges
   */
  public static VertexData createDefaultVertexWithEdges(final Long vertexID) {
    return createDefaultVertex(vertexID, GConstants.DEFAULT_VERTEX_LABEL, null,
      null);
  }

  /**
   * Creates a vertex with labels and outgoing edges.
   *
   * @param vertexID vertex identifier
   * @param label    vertex labels
   * @return vertex with identifier, labels and outgoing edges
   */
  public static VertexData createDefaultVertexWithLabel(final Long vertexID,
    final String label) {
    return createDefaultVertex(vertexID, label, null, null);
  }

  /**
   * Create a vertex with properties and outgoing edges.
   *
   * @param vertexID   vertex identifier
   * @param properties vertex properties
   * @return vertex with identifier and properties and outgoing edges
   */
  public static VertexData createDefaultVertexWithProperties(
    final Long vertexID, final String label,
    final Map<String, Object> properties) {
    return createDefaultVertex(vertexID, label, properties, null);
  }

  /**
   * Creates a vertex based on the given properties.
   *
   * @param id         vertex identifier
   * @param label      vertex labels
   * @param properties vertex properties
   * @param graphs     graphs that vertex belongs to
   * @return vertex
   */
  public static VertexData createDefaultVertex(final Long id,
    final String label, final Map<String, Object> properties,
    final Set<Long> graphs) {
    checkVertexID(id);
    return new DefaultVertexData(id, label, properties, graphs);
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
