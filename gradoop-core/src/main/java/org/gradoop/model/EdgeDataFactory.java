package org.gradoop.model;

import java.util.Map;
import java.util.Set;

public interface EdgeDataFactory<T extends EdgeData> extends
  EPGMElementFactory<T> {

  /**
   * Creates a default edge based on the given parameters.
   *
   * @param id             unique edge id
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @return edge connected to otherID with index
   */
  T createEdgeData(Long id, Long sourceVertexId, Long targetVertexId);

  /**
   * Creates a default edge based on the given parameters.
   *
   * @param id             unique edge id
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param label          edge label
   * @return edge data
   */
  T createEdgeData(Long id, String label, Long sourceVertexId,
    Long targetVertexId);

  /**
   * Creates a default edge based on the given parameters.
   *
   * @param id             unique edge id
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @return edge data
   */
  T createEdgeData(Long id, String label, Long sourceVertexId,
    Long targetVertexId, Map<String, Object> properties);

  /**
   * Creates a default edge based on the given parameters.
   *
   * @param id             unique edge id
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param graphs         graphs, that edge is contained in
   * @return edge data
   */
  T createEdgeData(Long id, String label, Long sourceVertexId,
    Long targetVertexId, Set<Long> graphs);

  /**
   * Creates a default edge based on the given parameters.
   *
   * @param id             unique edge id
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @param graphs         graphs, that edge is contained in
   * @return edge data
   */
  T createEdgeData(Long id, String label, Long sourceVertexId,
    Long targetVertexId, Map<String, Object> properties, Set<Long> graphs);
}
