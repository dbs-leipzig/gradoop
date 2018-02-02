/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.common.storage.impl.hbase;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.storage.api.PersistentVertex;

import java.util.Set;

/**
 * Represents a persistent vertex data object.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class HBaseVertex<V extends EPGMVertex, E extends EPGMEdge>
  extends HBaseGraphElement<V> implements PersistentVertex<E> {

  /**
   * Outgoing edge set
   */
  private Set<E> outgoingEdges;

  /**
   * Incoming edge set
   */
  private Set<E> incomingEdges;

  /**
   * Creates persistent vertex data.
   *
   * @param vertex        vertex
   * @param incomingEdges incoming edge
   * @param outgoingEdges outgoing edge
   */
  HBaseVertex(V vertex, Set<E> outgoingEdges, Set<E> incomingEdges) {
    super(vertex);
    this.outgoingEdges = outgoingEdges;
    this.incomingEdges = incomingEdges;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<E> getOutgoingEdges() {
    return outgoingEdges;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setOutgoingEdges(Set<E> outgoingEdgeIds) {
    this.outgoingEdges = outgoingEdgeIds;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<E> getIncomingEdges() {
    return incomingEdges;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setIncomingEdges(Set<E> incomingEdgeData) {
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
