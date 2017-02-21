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

package org.gradoop.examples.dimspan.dimspan.comparison;

import org.gradoop.examples.dimspan.dimspan.representation.DFSCodeUtils;

/**
 * Compare initial extensions of DFS codes in directed mode.
 */
public class DirectedDFSBranchComparator implements DFSBranchComparator {
  @Override
  public int compare(int[] a, int[] b) {
    int comparison = DFSCodeUtils.getFromLabel(a, 0) - DFSCodeUtils.getFromLabel(b, 0);

    if (comparison == 0) {
      boolean aIsLoop = DFSCodeUtils.isLoop(a, 0);
      if (aIsLoop == DFSCodeUtils.isLoop(b, 0)) {

        boolean aIsOutgoing = DFSCodeUtils.isOutgoing(a, 0);
        if (aIsOutgoing == DFSCodeUtils.isOutgoing(b, 0)) {

          comparison = DFSCodeUtils.getEdgeLabel(a, 0) - DFSCodeUtils.getEdgeLabel(b, 0);

          if (comparison == 0) {
            comparison = DFSCodeUtils.getToLabel(a, 0) - DFSCodeUtils.getToLabel(b, 0);
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
