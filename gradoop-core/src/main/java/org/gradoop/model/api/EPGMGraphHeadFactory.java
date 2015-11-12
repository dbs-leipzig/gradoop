package org.gradoop.model.api;

import org.gradoop.model.impl.id.GradoopId;

import java.util.Map;

/**
 * Creates {@link EPGMGraphHead} objects of a given type.
 *
 * @param <T> graph data type
 */
public interface EPGMGraphHeadFactory<T extends EPGMGraphHead> extends
  EPGMElementFactory<T> {

  /**
   * Creates graph data based on the given parameters.
   *
   * @param id graph identifier
   * @return graph data
   */
  T createGraphHead(GradoopId id);

  /**
   * Creates graph data based on the given parameters.
   *
   * @param id    graph identifier
   * @param label graph label
   * @return graph data
   */
  T createGraphHead(GradoopId id, String label);

  /**
   * Creates graph data based on the given parameters.
   *
   * @param id         graph identifier
   * @param label      graph label
   * @param properties graph attributes
   * @return graph data
   */
  T createGraphHead(GradoopId id, String label, Map<String, Object> properties);
}
