/*
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
