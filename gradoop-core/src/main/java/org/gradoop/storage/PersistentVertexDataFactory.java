package org.gradoop.storage;

import org.gradoop.model.EdgeData;
import org.gradoop.model.VertexData;

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
public interface PersistentVertexDataFactory<VD extends VertexData, ED
  extends EdgeData, PVD extends PersistentVertexData> extends
  Serializable {

  /**
   * Creates vertex data based on the given parameters.
   *
   * @param inputVertexData input vertex data
   * @param outgoingEdgeIds outgoing edge identifiers
   * @param incomingEdgeIds incoming edge identifiers
   * @return persistent vertex data
   */
  PVD createVertexData(VD inputVertexData, Set<ED> outgoingEdgeIds,
    Set<ED> incomingEdgeIds);
}
