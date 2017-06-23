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

/**
 * Util methods to interpret and manipulate int-array encoded graphs
 */
public interface SearchGraphUtils extends GraphUtils {

  /**
   * Find the fist edge greater than or equal to a given 1-edge DFS code.
   *
   * @param graphMux edge multiplex of the search space graph
   * @param searchMux edge multiplex of the branch's 1-edge DFS code
   * @param searchFromEdgeId offset of edge that are already known to be smaller
   *
   * @return first edge id with min DFS code greater than or equal search DFS code
   */
  int getFirstGeqEdgeId(int[] graphMux, int[] searchMux, int searchFromEdgeId);

  /**
   * Add an edge to a graph multiplex.
   *
   * @param mux graph multiplex
   * @param sourceId source id
   * @param sourceLabel source label
   * @param edgeLabel edge label
   * @param targetId target id
   * @param targetLabel target label
   *
   * @return updated graph multiplex
   */
  int[] addEdge(
    int[] mux, int sourceId, int sourceLabel, int edgeLabel, int targetId, int targetLabel);
}
