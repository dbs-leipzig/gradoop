package org.gradoop.model.api;

import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIds;

import java.util.Map;
import java.util.Set;

/**
 * Creates {@link EPGMEdge} objects of a given type.
 *
 * @param <T> edge data type
 */
public interface EPGMEdgeFactory<T extends EPGMEdge> extends
  EPGMElementFactory<T> {

  /**
   * Creates edge data based on the given parameters.
   *
   * @param id             unique edge id
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @return edge data
   */
  T createEdge(
    GradoopId id, GradoopId sourceVertexId, GradoopId targetVertexId);

  /**
   * Creates edge data based on the given parameters.
   *
   * @param id             unique edge id
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param label          edge label
   * @return edge data
   */
  T createEdge(GradoopId id, String label,
    GradoopId sourceVertexId, GradoopId targetVertexId);

  /**
   * Creates edge data based on the given parameters.
   *
   * @param id             unique edge id
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @return edge data
   */
  T createEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, Map<String, Object> properties);

  /**
   * Creates edge data based on the given parameters.
   *
   * @param id             unique edge id
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param graphIds         graphIds, that contain the edge
   * @return edge data
   */
  T createEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, GradoopIds graphIds);

  /**
   * Creates edge data based on the given parameters.
   *
   * @param id             unique edge id
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @param graphIds         graphIds, that contain the edge
   * @return edge data
   */
  T createEdge(GradoopId id, String label,
    GradoopId sourceVertexId, GradoopId targetVertexId,
    Map<String, Object> properties, GradoopIds graphIds);
}
