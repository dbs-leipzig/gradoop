
package org.gradoop.flink.algorithms.fsm.dimspan.comparison;


import org.gradoop.flink.algorithms.fsm.dimspan.model.DFSCodeUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.model.GraphUtilsBase;

/**
 * Compare initial extensions of DFS codes in directed mode.
 */
public class UndirectedDFSBranchComparator implements DFSBranchComparator {

  /**
   * util methods to interpret int-array encoded patterns
   */
  private GraphUtilsBase dfsCodeUtils = new DFSCodeUtils();

  @Override
  public int compare(int[] a, int[] b) {
    int comparison = dfsCodeUtils.getFromLabel(a, 0) - dfsCodeUtils.getFromLabel(b, 0);

    if (comparison == 0) {
      boolean aIsLoop = dfsCodeUtils.isLoop(a, 0);
      if (aIsLoop == dfsCodeUtils.isLoop(b, 0)) {
        comparison = dfsCodeUtils.getEdgeLabel(a, 0) - dfsCodeUtils.getEdgeLabel(b, 0);

        if (comparison == 0) {
          comparison = dfsCodeUtils.getToLabel(a, 0) - dfsCodeUtils.getToLabel(b, 0);
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
