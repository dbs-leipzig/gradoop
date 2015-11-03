package org.gradoop.storage.api;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;

import java.io.Serializable;
import java.util.Set;

/**
 * Base interface for creating persistent vertex data from transient vertex
 * data.
 *
 * @param <VD>  vertex data type
 * @param <ED>  edge  data type
 * @param <PVD> persistent vertex data type
 */
public interface PersistentVertexFactory<VD extends EPGMVertex, ED
  extends EPGMEdge, PVD extends PersistentVertex> extends
  Serializable {

  /**
   * Creates vertex data based on the given parameters.
   *
   * @param inputVertexData input vertex data
   * @param outgoingEdgeIds outgoing edge identifiers
   * @param incomingEdgeIds incoming edge identifiers
   * @return persistent vertex data
   */
  PVD createVertex(VD inputVertexData, Set<ED> outgoingEdgeIds,
    Set<ED> incomingEdgeIds);
}
