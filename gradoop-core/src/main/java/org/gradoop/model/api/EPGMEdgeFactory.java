package org.gradoop.model.api;

import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

import java.util.Map;

/**
 * Initializes {@link EPGMEdge} objects of a given type.
 *
 * @param <T> edge data type
 */
public interface EPGMEdgeFactory<T extends EPGMEdge> extends
  EPGMElementFactory<T> {

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @return edge data
   */
  T createEdge(GradoopId sourceVertexId,
    GradoopId targetVertexId);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             unique edge id
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @return edge data
   */
  T initEdge(GradoopId id,
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
  T createEdge(String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             unique edge id
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param label          edge label
   * @return edge data
   */
  T initEdge(GradoopId id,
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
  T createEdge(String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    Map<String, Object> properties);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             unique edge id
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @return edge data
   */
  T initEdge(GradoopId id,
    String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    Map<String, Object> properties);

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param graphIds         graphIds, that contain the edge
   * @return edge data
   */
  T createEdge(String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    GradoopIdSet graphIds);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             unique edge id
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param graphIds         graphIds, that contain the edge
   * @return edge data
   */
  T initEdge(GradoopId id,
    String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    GradoopIdSet graphIds);

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @param graphIds         graphIds, that contain the edge
   * @return edge data
   */
  T initEdge(String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    Map<String, Object> properties,
    GradoopIdSet graphIds);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             unique edge id
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @param graphIds         graphIds, that contain the edge
   * @return edge data
   */
  T initEdge(GradoopId id,
    String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    Map<String, Object> properties,
    GradoopIdSet graphIds);
}
