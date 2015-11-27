package org.gradoop.model.api;

import org.gradoop.model.impl.id.GradoopId;

/**
 * Initializes {@link EPGMGraphHead} objects of a given type.
 *
 * @param <T> graph data type
 */
public interface EPGMGraphHeadFactory<T extends EPGMGraphHead> extends
  EPGMElementFactory<T> {

  /**
   * Creates a new graph head based.
   *
   * @return graph data
   */
  T createGraphHead();

  /**
   * Initializes a graph head based on the given parameters.
   *
   * @param id graph identifier
   * @return graph data
   */
  T initGraphHead(GradoopId id);

  /**
   * Creates a new graph head based on the given parameters.
   *
   * @param label graph label
   * @return graph data
   */
  T createGraphHead(String label);

  /**
   * Initializes a graph head based on the given parameters.
   *
   * @param id    graph identifier
   * @param label graph label
   * @return graph data
   */
  T initGraphHead(GradoopId id, String label);

  /**
   * Creates a new graph head based on the given parameters.
   *
   * @param label      graph label
   * @param properties graph attributes
   * @return graph data
   */
  T createGraphHead(String label, EPGMProperties properties);

  /**
   * Initializes a graph head based on the given parameters.
   *
   * @param id         graph identifier
   * @param label      graph label
   * @param properties graph attributes
   * @return graph data
   */
  T initGraphHead(GradoopId id, String label, EPGMProperties properties);
}
