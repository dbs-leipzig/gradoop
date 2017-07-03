
package org.gradoop.common.storage.api;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;

/**
 * Representation of an edge on the storage level. We additionally store
 * vertex label information which enables filter mechanisms during loading.
 *
 * @param <V> EPGM vertex type
 */
public interface PersistentEdge<V extends EPGMVertex> extends EPGMEdge {

  /**
   * Loads the vertex data associated with the source vertex.
   *
   * @return source vertex data
   */
  V getSource();

  /**
   * Sets the vertex data associated with the source vertex.
   *
   * @param vertex source vertex data
   */
  void setSource(V vertex);

  /**
   * Loads the vertex data associated with the target vertex.
   *
   * @return target vertex data
   */
  V getTarget();

  /**
   * Sets the vertex data associated with the target vertex.
   *
   * @param vertex target vertex data
   */
  void setTarget(V vertex);

}
