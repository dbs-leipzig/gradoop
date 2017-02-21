package org.gradoop.examples.dimspan.dimspan.representation;


import org.gradoop.examples.dimspan.dimspan.comparison.DFSBranchComparator;
import org.gradoop.examples.dimspan.dimspan.comparison.DirectedDFSBranchComparator;
import org.gradoop.examples.dimspan.dimspan.comparison.UndirectedDFSBranchComparator;
import org.gradoop.examples.dimspan.dimspan.config.DIMSpanConfig;

public class SortedGraphUtils extends GraphUtilsBase implements GraphUtils {

  private final DFSBranchComparator comparator;

  public SortedGraphUtils(DIMSpanConfig fsmConfig) {
    this.comparator = fsmConfig.isDirected() ?
      new DirectedDFSBranchComparator() :
      new UndirectedDFSBranchComparator();
  }

  public int getFirstGeqEdgeId(int[] graph, int[] dfsCode, int searchFromEdgeId) {

    int firstGeqEdgeId = -1;

    for (int edgeId = searchFromEdgeId; edgeId < getEdgeCount(graph); edgeId++) {
      if (comparator.compare(dfsCode, getEdge(graph, edgeId)) >= 0) {
        firstGeqEdgeId = edgeId;
        break;
      }
    }

    return firstGeqEdgeId;
  }

  private int[] getEdge(int[] graph, int edgeId) {
    int[] edge = new int[EDGE_LENGTH];

    edge[FROM_ID] = getFromId(graph, edgeId);
    edge[TO_ID] = getToId(graph, edgeId);
    edge[FROM_LABEL] = getFromLabel(graph, edgeId);
    edge[TO_LABEL] = getToLabel(graph, edgeId);
    edge[DIRECTION] = isOutgoing(graph, edgeId) ? OUTGOING : INCOMING;
    edge[EDGE_LABEL] = getEdgeLabel(graph, edgeId);

    return edge;
  }
}
