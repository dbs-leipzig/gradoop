package org.gradoop.model;

import java.util.Map;

/**
 * Creates {@link GraphData} objects of a given type.
 *
 * @param <T> graph data type
 */
public interface GraphDataFactory<T extends GraphData> extends
  EPGMElementFactory<T> {

  /**
   * Creates graph data based on the given parameters.
   *
   * @param id graph identifier
   * @return graph data
   */
  T createGraphData(Long id);

  /**
   * Creates graph data based on the given parameters.
   *
   * @param id    graph identifier
   * @param label graph label
   * @return graph data
   */
  T createGraphData(Long id, String label);

  /**
   * Creates graph data based on the given parameters.
   *
   * @param id         graph identifier
   * @param label      graph label
   * @param properties graph attributes
   * @return graph data
   */
  T createGraphData(Long id, String label, Map<String, Object> properties);
}
