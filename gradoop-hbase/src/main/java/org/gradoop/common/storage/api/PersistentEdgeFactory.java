
package org.gradoop.common.storage.api;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;

import java.io.Serializable;

/**
 * Base interface for creating persistent edge data from transient edge data.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public interface PersistentEdgeFactory<E extends EPGMEdge, V extends EPGMVertex>
  extends Serializable {

  /**
   * Creates persistent edge data based on the given parameters.
   *
   * @param inputEdge    edge
   * @param sourceVertex source vertex
   * @param targetVertex target vertex
   * @return persistent edge
   */
  PersistentEdge<V> createEdge(E inputEdge, V sourceVertex, V targetVertex);
}
