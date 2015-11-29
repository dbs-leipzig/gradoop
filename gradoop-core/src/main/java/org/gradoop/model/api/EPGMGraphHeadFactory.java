package org.gradoop.model.api;

import org.gradoop.model.impl.id.GradoopId;

/**
 * Initializes {@link EPGMGraphHead} objects of a given type.
 *
 * @param <G> EPGM graph head type
 */
public interface EPGMGraphHeadFactory<G extends EPGMGraphHead>
  extends EPGMElementFactory<G> {

  /**
   * Creates a new graph head based.
   *
   * @return graph data
   */
  G createGraphHead();

  /**
   * Initializes a graph head based on the given parameters.
   *
   * @param id graph identifier
   * @return graph data
   */
  G initGraphHead(GradoopId id);

  /**
   * Creates a new graph head based on the given parameters.
   *
   * @param label graph label
   * @return graph data
   */
  G createGraphHead(String label);

  /**
   * Initializes a graph head based on the given parameters.
   *
   * @param id    graph identifier
   * @param label graph label
   * @return graph data
   */
  G initGraphHead(GradoopId id, String label);

  /**
   * Creates a new graph head based on the given parameters.
   *
   * @param label      graph label
   * @param properties graph attributes
   * @return graph data
   */
  G createGraphHead(String label, EPGMPropertyList properties);

  /**
   * Initializes a graph head based on the given parameters.
   *
   * @param id         graph identifier
   * @param label      graph label
   * @param properties graph attributes
   * @return graph data
   */
  G initGraphHead(GradoopId id, String label, EPGMPropertyList properties);
}
