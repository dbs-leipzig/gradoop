package org.gradoop.model.inmemory;

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
    return createDefaultEdge(otherID, GConstants.DEFAULT_EDGE_LABEL, index);
  }

  /**
   * Creates a default edge based on the given parameters.
   *
   * @param otherID the id of the vertex that edge is connected to
   * @param label   edge label
   * @param index   vertex centric edge index for parallel edges
   * @return edge connected to otherID with label and index
   */
  public static Edge createDefaultEdge(final Long otherID, final String label,
                                       final Long index) {
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
                                       final Long index,
                                       final Map<String, Object> properties) {
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

  private static void checkLabel(String label) {
    if (label == null || "".equals(label)) {
      throw new IllegalArgumentException("label must not be null or empty");
    }
  }
}
