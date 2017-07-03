
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
