package org.gradoop.model.api;

import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.properties.PropertyList;

/**
 * Initializes {@link EPGMVertex} objects of a given type.
 *
 * @param <V> EPGM vertex type
 */
public interface EPGMVertexFactory<V extends EPGMVertex> extends
  EPGMElementFactory<V> {
  /**
   * Initializes a new vertex based on the given parameters.
   *
   * @return vertex data
   */
  V createVertex();

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id vertex identifier
   * @return vertex data
   */
  V initVertex(GradoopId id);

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param label vertex label
   * @return vertex data
   */
  V createVertex(String label);

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id    vertex identifier
   * @param label vertex label
   * @return vertex data
   */
  V initVertex(GradoopId id,
    String label);

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param label      vertex labels
   * @param properties vertex properties
   * @return vertex data
   */
  V createVertex(String label,
    PropertyList properties);

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex labels
   * @param properties vertex properties
   * @return vertex data
   */
  V initVertex(GradoopId id,
    String label,
    PropertyList properties);

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param label  vertex label
   * @param graphIds graphIds, that contain the vertex
   * @return vertex data
   */
  V createVertex(String label,
    GradoopIdSet graphIds);

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id     vertex identifier
   * @param label  vertex label
   * @param graphIds graphIds, that contain the vertex
   * @return vertex data
   */
  V initVertex(GradoopId id,
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
  V createVertex(String label,
    PropertyList properties,
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
  V initVertex(GradoopId id,
    String label,
    PropertyList properties,
    GradoopIdSet graphIds);
}
