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

package org.gradoop.flink.algorithms.fsm.gspan.pojos;

import java.io.Serializable;

/**
 * pojo representing an adjacency list entry
 */
public class AdjacencyListEntry implements Serializable {
  /**
   * true, if edge is outgoing
   */
  private final boolean outgoing;
  /**
   * edge id
   */
  private final int edgeId;
  /**
   * edge label
   */
  private final int edgeLabel;
  /**
   * vertexId
   */
  private final int vertexId;
  /**
   * vertex label
   */
  private final int toVertexLabel;

  /**
   * constructor
   * @param outgoing true, if edge is outgoing
   * @param edgeId edge id
   * @param edgeLabel edge label
   * @param vertexId connected vertex id
   * @param toVertexLabel connected vertex label
   */
  public AdjacencyListEntry(boolean outgoing, int edgeId, int edgeLabel,
    int vertexId, int toVertexLabel) {

    this.outgoing = outgoing;
    this.edgeId = edgeId;
    this.edgeLabel = edgeLabel;
    this.vertexId = vertexId;
    this.toVertexLabel = toVertexLabel;
  }

  public int getToVertexLabel() {
    return toVertexLabel;
  }

  public boolean isOutgoing() {
    return outgoing;
  }

  public int getEdgeId() {
    return edgeId;
  }

  public int getEdgeLabel() {
    return edgeLabel;
  }

  public int getToVertexId() {
    return vertexId;
  }

  @Override
  public String toString() {
    return (outgoing ? "" : "<") +
      "-" + edgeId + ":" + edgeLabel + "-" +
      (outgoing ? ">" : "") +
      "(" + vertexId + ":" + toVertexLabel + ")";
  }
}
