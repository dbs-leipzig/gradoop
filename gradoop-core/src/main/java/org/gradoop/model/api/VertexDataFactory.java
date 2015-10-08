package org.gradoop.model.api;

import java.util.Map;
import java.util.Set;

/**
 * Creates {@link VertexData} objects of a given type.
 *
 * @param <T> vertex data type
 */
public interface VertexDataFactory<T extends VertexData> extends
  EPGMElementFactory<T> {
  /**
   * Creates vertex data based on the given parameters.
   *
   * @param id vertex identifier
   * @return vertex data
   */
  T createVertexData(Long id);

  /**
   * Creates vertex data based on the given parameters.
   *
   * @param id    vertex identifier
   * @param label vertex label
   * @return vertex data
   */
  T createVertexData(Long id, String label);

  /**
   * Creates vertex data based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex labels
   * @param properties vertex properties
   * @return vertex data
   */
  T createVertexData(Long id, String label, Map<String, Object> properties);

  /**
   * Creates vertex data based on the given parameters.
   *
   * @param id     vertex identifier
   * @param label  vertex label
   * @param graphs graphs, that contain the vertex
   * @return vertex data
   */
  T createVertexData(Long id, String label, Set<Long> graphs);

  /**
   * Creates vertex data based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex labels
   * @param properties vertex properties
   * @param graphs     graphs, that contain the vertex
   * @return vertex data
   */
  T createVertexData(Long id, String label, Map<String, Object> properties,
    Set<Long> graphs);
}
