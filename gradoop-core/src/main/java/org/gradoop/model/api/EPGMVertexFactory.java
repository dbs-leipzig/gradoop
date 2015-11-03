package org.gradoop.model.api;

import java.util.Map;
import java.util.Set;

/**
 * Creates {@link EPGMVertex} objects of a given type.
 *
 * @param <T> vertex data type
 */
public interface EPGMVertexFactory<T extends EPGMVertex> extends
  EPGMElementFactory<T> {
  /**
   * Creates vertex data based on the given parameters.
   *
   * @param id vertex identifier
   * @return vertex data
   */
  T createVertex(Long id);

  /**
   * Creates vertex data based on the given parameters.
   *
   * @param id    vertex identifier
   * @param label vertex label
   * @return vertex data
   */
  T createVertex(Long id, String label);

  /**
   * Creates vertex data based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex labels
   * @param properties vertex properties
   * @return vertex data
   */
  T createVertex(Long id, String label, Map<String, Object> properties);

  /**
   * Creates vertex data based on the given parameters.
   *
   * @param id     vertex identifier
   * @param label  vertex label
   * @param graphs graphs, that contain the vertex
   * @return vertex data
   */
  T createVertex(Long id, String label, Set<Long> graphs);

  /**
   * Creates vertex data based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex labels
   * @param properties vertex properties
   * @param graphs     graphs, that contain the vertex
   * @return vertex data
   */
  T createVertex(Long id, String label, Map<String, Object> properties,
    Set<Long> graphs);
}
