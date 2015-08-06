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
import org.gradoop.model.EdgeData;

import java.util.Map;
import java.util.Set;

/**
 * Factory for creating edges.
 */
public class EdgeDataFactory {

  /**
   * Avoid instantiation.
   */
  private EdgeDataFactory() {
  }

  /**
   * Creates a default edge based on the given parameters.
   *
   * @param id             unique edge id
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @return edge connected to otherID with index
   */
  public static EdgeData createDefaultEdge(final Long id,
    final Long sourceVertexId, final Long targetVertexId) {
    return createDefaultEdgeWithLabel(id, GConstants.DEFAULT_EDGE_LABEL,
      sourceVertexId, targetVertexId);
  }

  /**
   * Creates a default edge based on the given parameters.
   *
   * @param id             unique edge id
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @return edge data
   */
  public static EdgeData createDefaultEdgeWithLabel(final Long id,
    final String label, final Long sourceVertexId, final Long targetVertexId) {
    return createDefaultEdge(id, label, sourceVertexId, targetVertexId, null,
      null);
  }

  /**
   * Creates a default edge based on the given parameters.
   *
   * @param id             unique edge id
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param graphs         graphs, that edge is contained in
   * @return edge data
   */
  public static EdgeData createDefaultEdgeWithGraphs(final Long id,
    final String label, final Long sourceVertexId, final Long targetVertexId,
    Set<Long> graphs) {
    checkID(id);
    checkID(sourceVertexId);
    checkID(targetVertexId);
    checkLabel(label);
    return createDefaultEdge(id, label, sourceVertexId, targetVertexId, null,
      graphs);
  }

  /**
   * Creates a default edge based on the given parameters.
   *
   * @param id             unique edge id
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @param graphs         graphs, that edge is contained in
   * @return edge data
   */
  public static EdgeData createDefaultEdge(final Long id, final String label,
    final Long sourceVertexId, final Long targetVertexId,
    final Map<String, Object> properties, Set<Long> graphs) {
    checkID(id);
    checkID(sourceVertexId);
    checkID(targetVertexId);
    checkLabel(label);
    return new DefaultEdgeData(id, label, sourceVertexId, targetVertexId,
      properties, graphs);
  }

  /**
   * Checks if {@code otherID} is valid.
   *
   * @param otherID id of entity that edge points to
   */
  private static void checkID(Long otherID) {
    if (otherID == null) {
      throw new IllegalArgumentException(
        "edge-, source-, target-id must not be null");
    }
  }

  /**
   * Checks if {@code label} is valid (not null or empty).
   *
   * @param label edge label
   */
  private static void checkLabel(String label) {
    if (label == null || "".equals(label)) {
      throw new IllegalArgumentException("label must not be null or empty");
    }
  }
}
