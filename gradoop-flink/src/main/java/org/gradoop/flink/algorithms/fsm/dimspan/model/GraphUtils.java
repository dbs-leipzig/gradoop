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

package org.gradoop.flink.algorithms.fsm.dimspan.model;

import java.io.Serializable;

/**
 * Util methods to interpret and manipulate int-array encoded patterns and graphs
 */
public interface GraphUtils extends Serializable {

  /**
   * Number of indexes used to represent a single edge.
   */
  int EDGE_LENGTH = 6;

  /**
   * Offset of 1-edge DFS code's from label.
   */
  int FROM_LABEL = 0;

  /**
   * Offset of 1-edge DFS code's direction indicator.
   */
  int DIRECTION = 1;

  /**
   * Offset of 1-edge DFS code's edge label.
   */
  int EDGE_LABEL = 2;

  /**
   * Offset of 1-edge DFS code's to label.
   */
  int TO_LABEL = 3;

  /**
   * Offset of 1-edge DFS code's from id.
   */
  int FROM_ID = 4;

  /**
   * Offset of 1-edge DFS code's to id.
   */
  int TO_ID = 5;

  /**
   * integer model of "outgoing"
   */
  int OUTGOING = 0;

  /**
   * integer model of "incoming"
   */
  int INCOMING = 1;

  /**
   * Creates an integer multiplex representing a single edge traversal.
   *
   * @param fromId traversal from id
   * @param fromLabel traversal from label
   * @param outgoing traversal oin or against direction (0=outgoing)
   * @param edgeLabel label of the traversed edge
   * @param toId traversal to id
   * @param toLabel traversal to label
   *
   * @return integer multiplex representing the traversal
   */
  int[] multiplex(
    int fromId, int fromLabel, boolean outgoing, int edgeLabel, int toId, int toLabel);

  /**
   * Calculates the vertex count for a given edge multiplex.
   *
   * @param mux edge multiplex
   * @return vertex count
   */
  int getVertexCount(int[] mux);

  /**
   * Calculates the edge count for a given edge multiplex.
   *
   * @param mux edge multiplex
   * @return edge count
   */
  int getEdgeCount(int[] mux);

  /**
   * Returns distinct vertex labels from a given edge multiplex.
   *
   * @param mux edge multiplex
   * @return vertex labels
   */
  int[] getVertexLabels(int[] mux);

  /**
   * Getter.
   *
   * @param mux edge multiplex
   * @param edgeId edge id
   *
   * @return from vertex id
   */
  int getFromId(int[] mux, int edgeId);

  /**
   * Getter.
   *
   * @param mux edge multiplex
   * @param edgeId edge id
   *
   * @return from vertex label
   */
  int getFromLabel(int[] mux, int edgeId);

  /**
   * Getter.
   *
   * @param mux edge multiplex
   * @param edgeId edge id
   *
   * @return true, if edge was traversed in direction
   */
  boolean isOutgoing(int[] mux, int edgeId);

  /**
   * Getter.
   *
   * @param mux edge multiplex
   * @param edgeId edge id
   *
   * @return edge label
   */
  int getEdgeLabel(int[] mux, int edgeId);

  /**
   * Getter.
   *
   * @param mux edge multiplex
   * @param edgeId edge id
   *
   * @return to vertex id
   */
  int getToId(int[] mux, int edgeId);

  /**
   * Getter.
   *
   * @param mux edge multiplex
   * @param edgeId edge id
   *
   * @return to vertex label
   */
  int getToLabel(int[] mux, int edgeId);

  /**
   * Convenience method to check if an edge/extension is a loop.
   *
   * @param mux edge multiplex
   * @param edgeId edge id
   *
   * @return true, if edge/extension is a loop
   */
  boolean isLoop(int[] mux, int edgeId);
}
