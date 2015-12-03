package org.gradoop.storage.api;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;

import java.io.Serializable;
import java.util.Set;

/**
 * Base interface for creating persistent vertex data from transient vertex
 * data.
 *
 * @param <V>  EPGM vertex type
 * @param <E>  EPGM edge type
 * @param <PV> persistent vertex type
 */
public interface PersistentVertexFactory
  <V extends EPGMVertex, E extends EPGMEdge, PV extends PersistentVertex>
  extends Serializable {

  /**
   * Creates vertex data based on the given parameters.
   *
   * @param inputVertexData input vertex data
   * @param outgoingEdges   outgoing edge identifiers
   * @param incomingEdges   incoming edge identifiers
   * @return persistent vertex data
   */
  PV createVertex(
    V inputVertexData, Set<E> outgoingEdges, Set<E> incomingEdges);
}
