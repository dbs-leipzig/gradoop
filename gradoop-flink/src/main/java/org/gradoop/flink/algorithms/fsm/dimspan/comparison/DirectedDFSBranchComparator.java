
package org.gradoop.flink.algorithms.fsm.dimspan.comparison;

import org.gradoop.flink.algorithms.fsm.dimspan.model.DFSCodeUtils;

/**
 * Compare initial extensions of DFS codes in directed mode.
 */
public class DirectedDFSBranchComparator implements DFSBranchComparator {

  /**
   * util methods to interpret int-array encoded patterns
   */
  private DFSCodeUtils dfsCodeUtils = new DFSCodeUtils();

  @Override
  public int compare(int[] a, int[] b) {
    int comparison = dfsCodeUtils.getFromLabel(a, 0) - dfsCodeUtils.getFromLabel(b, 0);

    if (comparison == 0) {
      boolean aIsLoop = dfsCodeUtils.isLoop(a, 0);
      if (aIsLoop == dfsCodeUtils.isLoop(b, 0)) {

        boolean aIsOutgoing = dfsCodeUtils.isOutgoing(a, 0);
        if (aIsOutgoing == dfsCodeUtils.isOutgoing(b, 0)) {

          comparison = dfsCodeUtils.getEdgeLabel(a, 0) - dfsCodeUtils.getEdgeLabel(b, 0);

          if (comparison == 0) {
            comparison = dfsCodeUtils.getToLabel(a, 0) - dfsCodeUtils.getToLabel(b, 0);
          }
        } else {
          if (aIsOutgoing) {
            comparison = -1;
          } else {
            comparison = 1;
          }
        }
      } else {
        if (aIsLoop) {
          comparison = -1;
        } else {
          comparison = 1;
        }
      }
    }

    return comparison;
  }
}
