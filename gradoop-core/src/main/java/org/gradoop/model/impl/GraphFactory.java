package org.gradoop.model.impl;

import org.gradoop.GConstants;
import org.gradoop.model.Graph;

import java.util.Map;

/**
 * Factory for creating graphs.
 */
public class GraphFactory {

  /**
   * Avoid instantiation.
   */
  public GraphFactory() {
  }

  /**
   * Creates a graph using the given id.
   *
   * @param id graph identifier
   * @return graph with identifier
   */
  public static Graph createDefaultGraphWithID(final Long id) {
    return createDefaultGraph(id, null, null, null);
  }

  /**
   * Creates a graph using the given parameters.
   *
   * @param id         graph identifier
   * @param label      graph label
   * @param properties graph properties
   * @param vertices   vertices contained in the graph
   * @return graph
   */
  public static Graph createDefaultGraph(final Long id, final String label,
    final Map<String, Object> properties, final Iterable<Long> vertices) {
    checkGraphID(id);
    if (label == null || "".equals(label)) {
      return new DefaultGraph(id, GConstants.DEFAULT_GRAPH_LABEL, properties,
        vertices);
    } else {
      return new DefaultGraph(id, label, properties, vertices);
    }

  }

  /**
   * Checks if the given graphID is valid.
   *
   * @param graphID graph identifier
   */
  private static void checkGraphID(final Long graphID) {
    if (graphID == null) {
      throw new IllegalArgumentException("graphID must not be null");
    }
  }
}
