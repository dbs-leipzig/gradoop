package org.gradoop.model.impl;

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
   * @param labels     graph labels
   * @param properties graph properties
   * @param vertices   vertices contained in the graph
   * @return graph
   */
  public static Graph createDefaultGraph(final Long id,
                                         final Iterable<String> labels,
                                         final Map<String, Object> properties,
                                         final Iterable<Long> vertices) {
    checkGraphID(id);
    return new DefaultGraph(id, labels, properties, vertices);
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
