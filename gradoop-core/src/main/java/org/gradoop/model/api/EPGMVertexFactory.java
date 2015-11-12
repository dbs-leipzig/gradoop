package org.gradoop.model.api;

import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIds;

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
  T createVertex(GradoopId id);

  /**
   * Creates vertex data based on the given parameters.
   *
   * @param id    vertex identifier
   * @param label vertex label
   * @return vertex data
   */
  T createVertex(GradoopId id, String label);

  /**
   * Creates vertex data based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex labels
   * @param properties vertex properties
   * @return vertex data
   */
  T createVertex(GradoopId id, String label, Map<String, Object> properties);

  /**
   * Creates vertex data based on the given parameters.
   *
   * @param id     vertex identifier
   * @param label  vertex label
   * @param graphIds graphIds, that contain the vertex
   * @return vertex data
   */
  T createVertex(GradoopId id, String label, GradoopIds graphIds);

  /**
   * Creates vertex data based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex labels
   * @param properties vertex properties
   * @param graphIds     graphIds, that contain the vertex
   * @return vertex data
   */
  T createVertex(GradoopId id, String label,
    Map<String, Object> properties, GradoopIds graphIds);
}
