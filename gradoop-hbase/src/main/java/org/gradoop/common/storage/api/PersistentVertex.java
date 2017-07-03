
package org.gradoop.common.storage.api;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;

import java.util.Set;

/**
 * Representation of vertex data on the storage level. We additionally store
 * outgoing and incoming edges for faster access during e.g. traversal.
 *
 * @param <E> EPGM edge type
 */
public interface PersistentVertex<E extends EPGMEdge> extends EPGMVertex {

  /**
   * Returns outgoing edge data for the vertex.
   *
   * @return outgoing edge data
   */
  Set<E> getOutgoingEdges();

  /**
   * Sets outgoing edge data.
   *
   * @param outgoingEdgeData outgoing edge data
   */
  void setOutgoingEdges(Set<E> outgoingEdgeData);

  /**
   * Returns incoming edge data for the vertex.
   *
   * @return incoming edge data
   */
  Set<E> getIncomingEdges();

  /**
   * Sets incoming edge data.
   *
   * @param incomingEdgeData incoming edge data
   */
  void setIncomingEdges(Set<E> incomingEdgeData);

}
