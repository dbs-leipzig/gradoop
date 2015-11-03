/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.storage.impl.hbase;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.storage.api.PersistentEdge;

/**
 * Represents a persistent edge data object.
 */
public class HBaseEdge extends
  HBaseGraphElement<EPGMEdge> implements PersistentEdge<VertexPojo> {

  /**
   * Vertex data associated with the source vertex.
   */
  private VertexPojo sourceVertex;

  /**
   * Vertex data associated with the target vertex.
   */
  private VertexPojo targetVertex;

  /**
   * Default constructor.
   */
  public HBaseEdge() {
  }

  /**
   * Creates persistent edge data.
   *
   * @param edge         encapsulated edge data
   * @param sourceVertex source vertex data containing id and label
   * @param targetVertex target vertex data containing id and label
   */
  public HBaseEdge(EPGMEdge edge, VertexPojo sourceVertex,
    VertexPojo targetVertex) {
    super(edge);
    this.sourceVertex = sourceVertex;
    this.targetVertex = targetVertex;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexPojo getSourceVertex() {
    return sourceVertex;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setSourceVertex(VertexPojo sourceVertex) {
    this.sourceVertex = sourceVertex;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexPojo getTargetVertex() {
    return targetVertex;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setTargetVertex(VertexPojo targetVertex) {
    this.targetVertex = targetVertex;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Long getSourceVertexId() {
    return getEpgmElement().getSourceVertexId();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setSourceVertexId(Long sourceVertexId) {
    getEpgmElement().setSourceVertexId(sourceVertexId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Long getTargetVertexId() {
    return getEpgmElement().getTargetVertexId();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setTargetVertexId(Long targetVertexId) {
    getEpgmElement().setTargetVertexId(targetVertexId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HBaseEdge{");
    sb.append("super=").append(super.toString());
    sb.append(", sourceVertex=").append(sourceVertex);
    sb.append(", targetVertex=").append(targetVertex);
    sb.append('}');
    return sb.toString();
  }
}
