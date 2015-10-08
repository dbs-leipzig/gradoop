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

import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.pojo.DefaultEdgeData;
import org.gradoop.storage.api.PersistentVertexData;

import java.util.Set;

/**
 * Represents a persistent vertex data object.
 */
public class DefaultPersistentVertexData extends
  PersistentEPGMGraphElement<VertexData> implements
  PersistentVertexData<DefaultEdgeData> {

  /**
   * Edge data of outgoing edges.
   */
  private Set<DefaultEdgeData> outgoingEdgeData;

  /**
   * Edge data of incoming edges.
   */
  private Set<DefaultEdgeData> incomingEdgeData;

  /**
   * Default constructor.
   */
  public DefaultPersistentVertexData() {
  }

  /**
   * Creates persistent vertex data.
   *
   * @param vertexData       encapsulated vertex data
   * @param incomingEdgeData incoming edge data
   * @param outgoingEdgeData outgoing edge data
   */
  public DefaultPersistentVertexData(VertexData vertexData,
    Set<DefaultEdgeData> outgoingEdgeData,
    Set<DefaultEdgeData> incomingEdgeData) {
    super(vertexData);
    this.outgoingEdgeData = outgoingEdgeData;
    this.incomingEdgeData = incomingEdgeData;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<DefaultEdgeData> getOutgoingEdgeData() {
    return outgoingEdgeData;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setOutgoingEdgeData(Set<DefaultEdgeData> outgoingEdgeIds) {
    this.outgoingEdgeData = outgoingEdgeIds;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<DefaultEdgeData> getIncomingEdgeData() {
    return incomingEdgeData;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setIncomingEdgeData(Set<DefaultEdgeData> incomingEdgeData) {
    this.incomingEdgeData = incomingEdgeData;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("DefaultPersistentVertexData{");
    sb.append("super=").append(super.toString());
    sb.append(", outgoingEdgeData=").append(outgoingEdgeData);
    sb.append(", incomingEdgeData=").append(incomingEdgeData);
    sb.append('}');
    return sb.toString();
  }
}
