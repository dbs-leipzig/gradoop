
package org.gradoop.common.storage.api;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;

import java.io.Serializable;
import java.util.Set;

/**
 * Base interface for creating persistent vertex data from transient vertex
 * data.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public interface PersistentVertexFactory
  <V extends EPGMVertex, E extends EPGMEdge> extends Serializable {

  /**
   * Creates vertex data based on the given parameters.
   *
   * @param inputVertexData input vertex data
   * @param outgoingEdges   outgoing edge identifiers
   * @param incomingEdges   incoming edge identifiers
   * @return persistent vertex data
   */
  PersistentVertex<E> createVertex(V inputVertexData, Set<E> outgoingEdges,
    Set<E> incomingEdges);
}
