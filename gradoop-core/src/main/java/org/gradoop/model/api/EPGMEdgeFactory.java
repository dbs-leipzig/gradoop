package org.gradoop.model.api;

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
  T createEdge(Long id, Long sourceVertexId, Long targetVertexId);

  /**
   * Creates edge data based on the given parameters.
   *
   * @param id             unique edge id
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param label          edge label
   * @return edge data
   */
  T createEdge(Long id, String label, Long sourceVertexId, Long targetVertexId);

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
  T createEdge(Long id, String label, Long sourceVertexId, Long targetVertexId,
    Map<String, Object> properties);

  /**
   * Creates edge data based on the given parameters.
   *
   * @param id             unique edge id
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param graphs         graphs, that contain the edge
   * @return edge data
   */
  T createEdge(Long id, String label, Long sourceVertexId, Long targetVertexId,
    Set<Long> graphs);

  /**
   * Creates edge data based on the given parameters.
   *
   * @param id             unique edge id
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @param graphs         graphs, that contain the edge
   * @return edge data
   */
  T createEdge(Long id, String label, Long sourceVertexId, Long targetVertexId,
    Map<String, Object> properties, Set<Long> graphs);
}
