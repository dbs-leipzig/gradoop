package org.gradoop.storage.api;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;

import java.io.Serializable;

/**
 * Base interface for creating persistent edge data from transient edge data.
 *
 * @param <V>  EPGM vertex type
 * @param <E>  EPGM edge type
 * @param <PE> output persistent edge data type
 */
public interface PersistentEdgeFactory
  <V extends EPGMVertex, E extends EPGMEdge, PE extends PersistentEdge<V>>
  extends Serializable {

  /**
   * Creates persistent edge data based on the given parameters.
   *
   * @param inputEdge    edge
   * @param sourceVertex source vertex
   * @param targetVertex target vertex
   * @return persistent edge
   */
  PE createEdge(E inputEdge, V sourceVertex, V targetVertex);
}
