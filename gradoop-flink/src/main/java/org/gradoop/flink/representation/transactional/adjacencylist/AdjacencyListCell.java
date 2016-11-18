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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.representation.transactional.adjacencylist;

import org.gradoop.common.model.impl.id.GradoopId;

import java.io.Serializable;

/**
 * Entry of an adjacency list.
 * @param <T> type of algorithm specific cell value
 */
public class AdjacencyListCell<T> implements Serializable {

  /**
   * edge id
   */
  private GradoopId edgeId;
  /**
   * true, if outgoing, false, if incoming
   */
  private boolean outgoing;
  /**
   * target id (outgoing) or source id (incoming)
   */
  private GradoopId vertexId;
  /**
   * algorithm specific cell value
   */
  private T value;

  /**
   * Constructor.
   *  @param edgeId edge id
   * @param outgoing true, if outgoing, false, if incoming
   * @param vertexId target id (outgoing) or source id (incoming)
   * @param value algorithm-specific value
   */
  public AdjacencyListCell(GradoopId edgeId, boolean outgoing, GradoopId vertexId, T value) {
    this.outgoing = outgoing;
    this.edgeId = edgeId;
    this.vertexId = vertexId;
    this.value = value;
  }

  @Override
  public String toString() {
    return edgeId + (outgoing ? ">" : "<") + vertexId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AdjacencyListCell<?> that = (AdjacencyListCell<?>) o;

    if (outgoing != that.outgoing) {
      return false;
    }
    if (!edgeId.equals(that.edgeId)) {
      return false;
    }
    return value != null ? value.equals(that.value) : that.value == null;

  }

  @Override
  public int hashCode() {
    int result = edgeId.hashCode();
    result = 31 * result + (outgoing ? 1 : 0);
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }

  public GradoopId getEdgeId() {
    return edgeId;
  }

  public void setEdgeId(GradoopId edgeId) {
    this.edgeId = edgeId;
  }

  public boolean isOutgoing() {
    return outgoing;
  }

  public void setOutgoing(boolean outgoing) {
    this.outgoing = outgoing;
  }

  public GradoopId getVertexId() {
    return vertexId;
  }

  public void setVertexId(GradoopId vertexId) {
    this.vertexId = vertexId;
  }

  public T getValue() {
    return value;
  }

  public void setValue(T value) {
    this.value = value;
  }
}
