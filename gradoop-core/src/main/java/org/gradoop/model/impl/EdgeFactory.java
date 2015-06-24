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
import org.gradoop.model.Edge;

import java.util.Map;

/**
 * Factory for creating edges.
 */
public class EdgeFactory {

  /**
   * Avoid instantiation.
   */
  private EdgeFactory() {
  }

  /**
   * Creates a default edge based on the given parameters.
   *
   * @param otherID the id of the vertex that edge is connected to
   * @param index   vertex centric edge index for parallel edges
   * @return edge connected to otherID with index
   */
  public static Edge createDefaultEdge(final Long otherID, final Long index) {
    return createDefaultEdgeWithLabel(otherID, GConstants.DEFAULT_EDGE_LABEL,
      index);
  }

  /**
   * Creates a default edge based on the given parameters.
   *
   * @param otherID the id of the vertex that edge is connected to
   * @param label   edge label
   * @param index   vertex centric edge index for parallel edges
   * @return edge connected to otherID with label and index
   */
  public static Edge createDefaultEdgeWithLabel(final Long otherID,
    final String label, final Long index) {
    return createDefaultEdge(otherID, label, index, null);
  }

  /**
   * Creates a default edge based on the given parameters.
   *
   * @param otherID    the id of the vertex that edge is connected to
   * @param label      edge label
   * @param index      vertex centric edge index for parallel edges
   * @param properties edge properties
   * @return edge connected to otherID with label, properties and index
   */
  public static Edge createDefaultEdge(final Long otherID, final String label,
    final Long index, final Map<String, Object> properties) {
    checkID(otherID);
    checkIndex(index);
    checkLabel(label);
    return new DefaultEdge(otherID, label, index, properties);
  }

  /**
   * Checks if {@code otherID} is valid.
   *
   * @param otherID id of entity that edge points to
   */
  private static void checkID(Long otherID) {
    if (otherID == null) {
      throw new IllegalArgumentException("otherID must not be null");
    }
  }

  /**
   * Checks if {@code index} is valid.
   *
   * @param index internal index of edge
   */
  private static void checkIndex(Long index) {
    if (index == null) {
      throw new IllegalArgumentException("index must not be null");
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
