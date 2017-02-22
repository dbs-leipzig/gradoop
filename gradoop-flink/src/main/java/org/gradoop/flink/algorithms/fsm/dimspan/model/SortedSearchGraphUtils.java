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

import org.gradoop.flink.algorithms.fsm.dimspan.comparison.DFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.dimspan.comparison.DirectedDFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.dimspan.comparison.UndirectedDFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;

/**
 * Util methods to interpret and manipulate sorted int-array encoded graphs
 */
public class SortedSearchGraphUtils extends SearchGraphUtilsBase {

  /**
   * Comparator used to search for first edge greater than or equal to a branch DFS code.
   */
  private final DFSBranchComparator comparator;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public SortedSearchGraphUtils(DIMSpanConfig fsmConfig) {
    this.comparator = fsmConfig.isDirected() ?
      new DirectedDFSBranchComparator() :
      new UndirectedDFSBranchComparator();
  }

  @Override
  public int getFirstGeqEdgeId(int[] graphMux, int[] searchMux, int searchFromEdgeId) {

    int firstGeqEdgeId = -1;

    for (int edgeId = searchFromEdgeId; edgeId < getEdgeCount(graphMux); edgeId++) {
      if (comparator.compare(searchMux, getEdge(graphMux, edgeId)) >= 0) {
        firstGeqEdgeId = edgeId;
        break;
      }
    }

    return firstGeqEdgeId;
  }

  /**
   * Returns the multiplex of a single edge.
   *
   * @param graphMux multiplex of all edges
   * @param edgeId edge id
   * @return multiplex of edge with given id
   */
  private int[] getEdge(int[] graphMux, int edgeId) {
    int[] edgeMux = new int[EDGE_LENGTH];

    edgeMux[FROM_ID] = getFromId(graphMux, edgeId);
    edgeMux[TO_ID] = getToId(graphMux, edgeId);
    edgeMux[FROM_LABEL] = getFromLabel(graphMux, edgeId);
    edgeMux[TO_LABEL] = getToLabel(graphMux, edgeId);
    edgeMux[DIRECTION] = isOutgoing(graphMux, edgeId) ? OUTGOING : INCOMING;
    edgeMux[EDGE_LABEL] = getEdgeLabel(graphMux, edgeId);

    return edgeMux;
  }
}
