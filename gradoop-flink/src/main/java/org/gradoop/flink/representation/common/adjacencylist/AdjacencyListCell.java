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

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Entry of an adjacency list.
 * @param <VD> vertex data
 * @param <ED> edge data
 */
public class AdjacencyListCell<ED, VD> extends Tuple2<ED, VD> {

  /**
   * Constructor.
   * @param edgeData edge id
   * @param vertexData target id (outgoing) or source id (incoming)
   */
  public AdjacencyListCell(ED edgeData, VD vertexData) {
    this.f0 = edgeData;
    this.f1 = vertexData;
  }

  public ED getEdgeData() {
    return f0;
  }

  public void setEdgeData(ED f0) {
    this.f0 = f0;
  }

  public VD getVertexData() {
    return f1;
  }

  public void setVertexData(VD f1) {
    this.f1 = f1;
  }

}