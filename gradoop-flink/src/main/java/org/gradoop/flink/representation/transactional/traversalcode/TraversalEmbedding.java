
package org.gradoop.flink.representation.transactional.traversalcode;

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.List;

/**
 * Mapping between an embedding and a DFS code.
 */
public class TraversalEmbedding {

  /**
   * Initial vertex discovery times.
   */
  private final List<GradoopId> vertexIds;

  /**
   * Included edges.
   */
  private final List<GradoopId> edgeIds;

  /**
   * Constructor.
   *
   * @param vertexIds vertex ids
   * @param edgeIds edge ids
   */
  public TraversalEmbedding(List<GradoopId> vertexIds, List<GradoopId> edgeIds) {
    this.vertexIds = vertexIds;
    this.edgeIds = edgeIds;
  }

  /**
   * Default constructor.
   */
  public TraversalEmbedding() {
    this.vertexIds = Lists.newArrayList();
    this.edgeIds = Lists.newArrayList();
  }

  /**
   * Constructor.
   *
   * @param parent parent embedding
   */
  public TraversalEmbedding(TraversalEmbedding parent) {
    this.vertexIds = Lists.newArrayList(parent.getVertexIds());
    this.edgeIds = Lists.newArrayList(parent.getEdgeIds());
  }

  public List<GradoopId> getEdgeIds() {
    return edgeIds;
  }

  public List<GradoopId> getVertexIds() {
    return vertexIds;
  }

  @Override
  public String toString() {
    return vertexIds + ";" + edgeIds;
  }
}
