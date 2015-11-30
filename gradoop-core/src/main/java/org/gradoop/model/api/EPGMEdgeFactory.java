package org.gradoop.model.api;

import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.properties.PropertyList;

/**
 * Initializes {@link EPGMEdge} objects of a given type.
 *
 * @param <E> EPGM edge type
 */
public interface EPGMEdgeFactory<E extends EPGMEdge>
  extends EPGMElementFactory<E> {

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @return edge data
   */
  E createEdge(GradoopId sourceVertexId,
    GradoopId targetVertexId);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @return edge data
   */
  E initEdge(GradoopId id,
    GradoopId sourceVertexId,
    GradoopId targetVertexId);

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param label          edge label
   * @return edge data
   */
  E createEdge(String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param label          edge label
   * @return edge data
   */
  E initEdge(GradoopId id,
    String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId);

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @return edge data
   */
  E createEdge(String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    PropertyList properties);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @return edge data
   */
  E initEdge(GradoopId id,
    String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    PropertyList properties);

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param label           edge label
   * @param sourceVertexId  source vertex id
   * @param targetVertexId  target vertex id
   * @param graphIds        graphIds, that contain the edge
   * @return edge data
   */
  E createEdge(String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    GradoopIdSet graphIds);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id              edge identifier
   * @param label           edge label
   * @param sourceVertexId  source vertex id
   * @param targetVertexId  target vertex id
   * @param graphIds        graphIds, that contain the edge
   * @return edge data
   */
  E initEdge(GradoopId id,
    String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    GradoopIdSet graphIds);

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param label           edge label
   * @param sourceVertexId  source vertex id
   * @param targetVertexId  target vertex id
   * @param properties      edge properties
   * @param graphIds        graphIds, that contain the edge
   * @return edge data
   */
  E createEdge(String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    PropertyList properties,
    GradoopIdSet graphIds);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id              edge identifier
   * @param label           edge label
   * @param sourceVertexId  source vertex id
   * @param targetVertexId  target vertex id
   * @param properties      edge properties
   * @param graphIds        graphIds, that contain the edge
   * @return edge data
   */
  E initEdge(GradoopId id,
    String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    PropertyList properties,
    GradoopIdSet graphIds);
}
