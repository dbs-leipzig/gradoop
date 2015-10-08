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

import org.gradoop.model.api.EdgeData;
import org.gradoop.model.impl.pojo.DefaultVertexData;
import org.gradoop.storage.api.PersistentEdgeData;

/**
 * Represents a persistent edge data object.
 */
public class DefaultPersistentEdgeData extends
  PersistentEPGMGraphElement<EdgeData> implements
  PersistentEdgeData<DefaultVertexData> {

  /**
   * Vertex data associated with the source vertex.
   */
  private DefaultVertexData sourceVertexData;

  /**
   * Vertex data associated with the target vertex.
   */
  private DefaultVertexData targetVertexData;

  /**
   * Default constructor.
   */
  public DefaultPersistentEdgeData() {
  }

  /**
   * Creates persistent edge data.
   *
   * @param edgeData         encapsulated edge data
   * @param sourceVertexData source vertex data containing id and label
   * @param targetVertexData target vertex data containing id and label
   */
  public DefaultPersistentEdgeData(EdgeData edgeData,
    DefaultVertexData sourceVertexData, DefaultVertexData targetVertexData) {
    super(edgeData);
    this.sourceVertexData = sourceVertexData;
    this.targetVertexData = targetVertexData;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DefaultVertexData getSourceVertexData() {
    return sourceVertexData;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setSourceVertexData(DefaultVertexData sourceVertexData) {
    this.sourceVertexData = sourceVertexData;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DefaultVertexData getTargetVertexData() {
    return targetVertexData;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setTargetVertexData(DefaultVertexData targetVertexData) {
    this.targetVertexData = targetVertexData;
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
    final StringBuilder sb = new StringBuilder("DefaultPersistentEdgeData{");
    sb.append("super=").append(super.toString());
    sb.append(", sourceVertexData=").append(sourceVertexData);
    sb.append(", targetVertexData=").append(targetVertexData);
    sb.append('}');
    return sb.toString();
  }
}
