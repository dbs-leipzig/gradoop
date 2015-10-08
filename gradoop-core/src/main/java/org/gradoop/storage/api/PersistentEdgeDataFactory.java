package org.gradoop.storage.api;

import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.VertexData;

import java.io.Serializable;

/**
 * Base interface for creating persistent edge data from transient edge data.
 *
 * @param <ED>  input edge data type
 * @param <VD>  input vertex data type
 * @param <PED> output persistent edge data type
 */
public interface PersistentEdgeDataFactory<ED extends EdgeData, VD extends
  VertexData, PED extends PersistentEdgeData<VD>> extends
  Serializable {

  /**
   * Creates persistent edge data based on the given parameters.
   *
   * @param inputEdgeData    transient edge data
   * @param sourceVertexData source vertex data
   * @param targetVertexData target vertex data
   * @return edge data
   */
  PED createEdgeData(ED inputEdgeData, VD sourceVertexData,
    VD targetVertexData);
}
