package org.gradoop.model;

import java.util.Map;
import java.util.Set;

public interface GraphDataFactory<T extends GraphData> extends
  EPGMElementFactory<T> {

  /**
   * Creates a graph with default label.
   *
   * @param id graph identifier
   * @return graph with identifier
   */
  T createGraphData(Long id);

  /**
   * Creates a labelled graph.
   *
   * @param id    graph identifier
   * @param label graph label
   * @return labelled graph with identifier
   */
  T createGraphData(Long id, String label);

  /**
   * Creates a labelled, attributed graph.
   *
   * @param id         graph identifier
   * @param label      graph label
   * @param properties graph attributes
   * @return labelled, attributed graph with identifier
   */
  T createGraphData(Long id, String label, Map<String, Object> properties);

  /**
   * Creates a labelled, attributed graph.
   *
   * @param id       graph identifier
   * @param label    graph label
   * @param vertices vertices contained in that graph
   * @param edges    edges contained in that graph
   * @return labelled graph with identifier
   */
  T createGraphData(Long id, String label, Set<Long> vertices, Set<Long> edges);

  /**
   * Creates a labelled, attributed graph.
   *
   * @param id         graph identifier
   * @param label      graph label
   * @param properties graph attributes
   * @param vertices   vertices contained in that graph
   * @param edges      edges contained in that graph
   * @return labelled, attributed graph with identifier
   */
  T createGraphData(Long id, String label, Map<String, Object> properties,
    Set<Long> vertices, Set<Long> edges);
}
