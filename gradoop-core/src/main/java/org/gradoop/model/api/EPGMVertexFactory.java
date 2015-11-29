package org.gradoop.model.api;

import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

/**
 * Initializes {@link EPGMVertex} objects of a given type.
 *
 * @param <T> vertex data type
 */
public interface EPGMVertexFactory<T extends EPGMVertex> extends
  EPGMElementFactory<T> {
  /**
   * Initializes a new vertex based on the given parameters.
   *
   * @return vertex data
   */
  T createVertex();

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id vertex identifier
   * @return vertex data
   */
  T initVertex(GradoopId id);

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param label vertex label
   * @return vertex data
   */
  T createVertex(String label);

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id    vertex identifier
   * @param label vertex label
   * @return vertex data
   */
  T initVertex(GradoopId id,
    String label);

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param label      vertex labels
   * @param properties vertex properties
   * @return vertex data
   */
  T createVertex(String label,
    EPGMPropertyList properties);

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex labels
   * @param properties vertex properties
   * @return vertex data
   */
  T initVertex(GradoopId id,
    String label,
    EPGMPropertyList properties);

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param label  vertex label
   * @param graphIds graphIds, that contain the vertex
   * @return vertex data
   */
  T createVertex(String label,
    GradoopIdSet graphIds);

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id     vertex identifier
   * @param label  vertex label
   * @param graphIds graphIds, that contain the vertex
   * @return vertex data
   */
  T initVertex(GradoopId id,
    String label,
    GradoopIdSet graphIds);

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param label      vertex labels
   * @param properties vertex properties
   * @param graphIds     graphIds, that contain the vertex
   * @return vertex data
   */
  T createVertex(String label,
    EPGMPropertyList properties,
    GradoopIdSet graphIds);

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex labels
   * @param properties vertex properties
   * @param graphIds     graphIds, that contain the vertex
   * @return vertex data
   */
  T initVertex(GradoopId id,
    String label,
    EPGMPropertyList properties,
    GradoopIdSet graphIds);
}
