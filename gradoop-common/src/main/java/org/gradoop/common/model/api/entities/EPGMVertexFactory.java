
package org.gradoop.common.model.api.entities;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Initializes {@link EPGMVertex} objects of a given type.
 *
 * @param <V> EPGM vertex type
 */
public interface EPGMVertexFactory<V extends EPGMVertex>
  extends EPGMElementFactory<V> {

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
  V initVertex(GradoopId id, String label);

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param label      vertex labels
   * @param properties vertex properties
   * @return vertex data
   */
  V createVertex(String label, Properties properties);

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex labels
   * @param properties vertex properties
   * @return vertex data
   */
  V initVertex(GradoopId id, String label, Properties properties);

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param label  vertex label
   * @param graphIds graphIds, that contain the vertex
   * @return vertex data
   */
  V createVertex(String label, GradoopIdList graphIds);

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id     vertex identifier
   * @param label  vertex label
   * @param graphIds graphIds, that contain the vertex
   * @return vertex data
   */
  V initVertex(GradoopId id, String label, GradoopIdList graphIds);

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param label      vertex labels
   * @param properties vertex properties
   * @param graphIds     graphIds, that contain the vertex
   * @return vertex data
   */
  V createVertex(String label, Properties properties, GradoopIdList graphIds);

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex labels
   * @param properties vertex properties
   * @param graphIds     graphIds, that contain the vertex
   * @return vertex data
   */
  V initVertex(GradoopId id, String label, Properties properties,
    GradoopIdList graphIds);
}
