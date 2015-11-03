package org.gradoop.storage.api;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;

import java.io.Serializable;

/**
 * Base interface for creating persistent edge data from transient edge data.
 *
 * @param <ED>  input edge data type
 * @param <VD>  input vertex data type
 * @param <PED> output persistent edge data type
 */
public interface PersistentEdgeFactory<ED extends EPGMEdge, VD extends EPGMVertex, PED extends PersistentEdge<VD>> extends
  Serializable {

  /**
   * Creates persistent edge data based on the given parameters.
   *
   * @param inputEdgeData    transient edge data
   * @param sourceVertexData source vertex data
   * @param targetVertexData target vertex data
   * @return edge data
   */
  PED createEdge(ED inputEdgeData, VD sourceVertexData, VD targetVertexData);
}
