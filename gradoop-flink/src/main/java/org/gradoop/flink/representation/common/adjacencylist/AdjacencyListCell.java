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

package org.gradoop.flink.representation.common.adjacencylist;

import java.io.Serializable;

/**
 * Entry of an adjacency list.
 * @param <VD> vertex data
 * @param <ED> edge data
 */
public class AdjacencyListCell<ED, VD> implements Serializable {

  /**
   * edge data
   */
  private ED edgeData;

  /**
   * referenced vertex data
   */
  private VD vertexData;

  /**
   * Constructor.
   * @param edgeData edge id
   * @param vertexData target id (outgoing) or source id (incoming)
   */
  public AdjacencyListCell(ED edgeData, VD vertexData) {
    this.edgeData = edgeData;
    this.vertexData = vertexData;
  }

  @Override
  public String toString() {
    return edgeData.toString() + vertexData;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AdjacencyListCell<?, ?> that = (AdjacencyListCell<?, ?>) o;

    if (!edgeData.equals(that.edgeData)) {
      return false;
    }
    return vertexData.equals(that.vertexData);
  }

  @Override
  public int hashCode() {
    int result = edgeData.hashCode();
    result = 31 * result + vertexData.hashCode();
    return result;
  }

  public ED getEdgeData() {
    return edgeData;
  }

  public void setEdgeData(ED edgeData) {
    this.edgeData = edgeData;
  }

  public VD getVertexData() {
    return vertexData;
  }

  public void setVertexData(VD vertexData) {
    this.vertexData = vertexData;
  }

}
