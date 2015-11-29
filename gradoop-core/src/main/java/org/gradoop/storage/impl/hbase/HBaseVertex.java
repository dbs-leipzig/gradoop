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

import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.storage.api.PersistentVertex;

import java.util.Set;

/**
 * Represents a persistent vertex data object.
 */
public class HBaseVertex extends
  HBaseGraphElement<EPGMVertex> implements PersistentVertex<EdgePojo> {

  /**
   * Outgoing edge set
   */
  private Set<EdgePojo> outgoingEdges;

  /**
   * Incoming edge set
   */
  private Set<EdgePojo> incomingEdges;

  /**
   * Creates persistent vertex data.
   *
   * @param vertex        vertex
   * @param incomingEdges incoming edge
   * @param outgoingEdges outgoing edge
   */
  HBaseVertex(EPGMVertex vertex, Set<EdgePojo> outgoingEdges,
    Set<EdgePojo> incomingEdges) {
    super(vertex);
    this.outgoingEdges = outgoingEdges;
    this.incomingEdges = incomingEdges;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<EdgePojo> getOutgoingEdges() {
    return outgoingEdges;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setOutgoingEdges(Set<EdgePojo> outgoingEdgeIds) {
    this.outgoingEdges = outgoingEdgeIds;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<EdgePojo> getIncomingEdges() {
    return incomingEdges;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setIncomingEdges(Set<EdgePojo> incomingEdgeData) {
    this.incomingEdges = incomingEdgeData;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HBaseVertex{");
    sb.append("super=").append(super.toString());
    sb.append(", outgoingEdges=").append(outgoingEdges);
    sb.append(", incomingEdges=").append(incomingEdges);
    sb.append('}');
    return sb.toString();
  }
}
