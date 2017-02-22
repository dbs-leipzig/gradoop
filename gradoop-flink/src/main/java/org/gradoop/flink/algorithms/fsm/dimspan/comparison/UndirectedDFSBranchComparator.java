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

package org.gradoop.flink.algorithms.fsm.dimspan.comparison;


import org.gradoop.flink.algorithms.fsm.dimspan.model.DFSCodeUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.model.GraphUtilsBase;

/**
 * Compare initial extensions of DFS codes in directed mode.
 */
public class UndirectedDFSBranchComparator implements DFSBranchComparator {
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
