package org.gradoop.model;

import java.util.Map;
import java.util.Set;

public interface VertexDataFactory<T extends VertexData> extends
  EPGMElementFactory<T> {
  /**
   * Creates a vertex with default label.
   *
   * @param id vertex identifier
   * @return vertex with identifier
   */
  T createVertexData(Long id);

  /**
   * Creates a labelled vertex.
   *
   * @param id vertex identifier
   * @return vertex with identifier and label.
   */
  T createVertexData(Long id, String label);

  /**
   * Creates a labelled vertex with properties.
   *
   * @param id         vertex identifier
   * @param label      vertex labels
   * @param properties vertex properties
   * @return vertex with identifier, labels and outgoing edges
   */
  T createVertexData(Long id, String label, Map<String, Object> properties);

  /**
   * Create a labelled vertex with logical graphs.
   *
   * @param id     vertex identifier
   * @param graphs graphs that vertex belongs to
   * @return vertex with identifier and properties and outgoing edges
   */
  T createVertexData(Long id, String label, Set<Long> graphs);

  /**
   * Creates a labelled vertex with properties and graphs.
   *
   * @param id         vertex identifier
   * @param label      vertex labels
   * @param properties vertex properties
   * @param graphs     graphs that vertex belongs to
   * @return vertex
   */
  T createVertexData(Long id, String label, Map<String, Object> properties,
    Set<Long> graphs);
}
