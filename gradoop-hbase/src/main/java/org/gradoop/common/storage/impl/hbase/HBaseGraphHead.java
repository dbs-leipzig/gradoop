
package org.gradoop.common.storage.impl.hbase;

import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.storage.api.PersistentGraphHead;

/**
 * Represents a persistent vertex data object.
 *
 * @param <G> EPGM graph head type
 */
public class HBaseGraphHead<G extends EPGMGraphHead> extends HBaseElement<G>
  implements PersistentGraphHead {

  /**
   * EPGMVertex identifiers contained in that logical graph.
   */
  private GradoopIdList vertexIds;

  /**
   * EPGMEdge identifiers contained in that logical graph.
   */
  private GradoopIdList edgeIds;

  /**
   * Creates  persistent graph data.
   *
   * @param graphHead encapsulated graph data
   * @param vertexIds  vertexIds contained in that graph
   * @param edgeIds     edgeIds contained in that graph
   */
  HBaseGraphHead(G graphHead, GradoopIdList vertexIds,
    GradoopIdList edgeIds) {
    super(graphHead);
    this.vertexIds = vertexIds;
    this.edgeIds = edgeIds;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopIdList getVertexIds() {
    return vertexIds;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setVertexIds(GradoopIdList vertices) {
    this.vertexIds = vertices;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addVertexId(GradoopId vertexID) {
    if (vertexIds != null) {
      vertexIds.add(vertexID);
    } else {
      vertexIds = GradoopIdList.fromExisting(vertexID);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getVertexCount() {
    return (vertexIds != null) ? vertexIds.size() : 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopIdList getEdgeIds() {
    return edgeIds;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setEdgeIds(GradoopIdList edgeIds) {
    this.edgeIds = edgeIds;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addEdgeId(GradoopId edgeID) {
    if (edgeIds != null) {
      edgeIds.add(edgeID);
    } else {
      edgeIds = GradoopIdList.fromExisting(edgeID);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getEdgeCount() {
    return (edgeIds != null) ? edgeIds.size() : 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HBaseGraphHead{");
    sb.append("super=").append(super.toString());
    sb.append(", vertexIds=").append(vertexIds);
    sb.append(", edgeIds=").append(edgeIds);
    sb.append('}');
    return sb.toString();
  }
}
