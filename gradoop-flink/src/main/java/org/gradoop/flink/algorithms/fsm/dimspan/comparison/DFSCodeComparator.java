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

import java.io.Serializable;
import java.util.Comparator;

/**
 * Comparator for DFS codes based on gSpan lexicographical order.
 * See <a href="https://www.cs.ucsb.edu/~xyan/software/gSpan.htm">gSpan</a>
 */
public class DFSCodeComparator implements Comparator<int[]>, Serializable {

  @Override
  public int compare(int[] a, int[] b) {
    int comparison;

    boolean thisIsRoot = a.length == 0;
    boolean thatIsRoot = b.length == 0;

    // root is always smaller
    if (thisIsRoot && ! thatIsRoot) {
      comparison = -1;

    } else if (thatIsRoot && !thisIsRoot) {
      comparison = 1;

      // none is root
    } else {
      comparison = 0;

      int thisSize = DFSCodeUtils.getEdgeCount(a);
      int thatSize = DFSCodeUtils.getEdgeCount(b);

      boolean sameSize = thisSize == thatSize;

      int minSize  = sameSize || thisSize < thatSize ? thisSize : thatSize;

      // compare extensions
      for (int edgeTime = 0; edgeTime < minSize; edgeTime++) {

        int thisFromTime = DFSCodeUtils.getFromId(a, edgeTime);
        int thatFromTime = DFSCodeUtils.getFromId(b, edgeTime);

        int thisToTime = DFSCodeUtils.getToId(a, edgeTime);
        int thatToTime = DFSCodeUtils.getToId(b, edgeTime);

        // no difference is times
        if (thisFromTime == thatFromTime && thisToTime == thatToTime) {

          // compare from Labels
          int thisFromLabel = DFSCodeUtils.getFromLabel(a, edgeTime);
          int thatFromLabel = DFSCodeUtils.getFromLabel(b, edgeTime);

          comparison = thisFromLabel - thatFromLabel;

          if (comparison == 0) {

            // compare direction
            boolean thisIsOutgoing = DFSCodeUtils.isOutgoing(a, edgeTime);
            boolean thatIsOutgoing = DFSCodeUtils.isOutgoing(b, edgeTime);

            if (thisIsOutgoing && !thatIsOutgoing) {
              comparison = -1;
            } else if (thatIsOutgoing && !thisIsOutgoing) {
              comparison = 1;
            } else {

              // compare edge Labels
              int thisEdgeLabel = DFSCodeUtils.getEdgeLabel(a, edgeTime);
              int thatEdgeLabel = DFSCodeUtils.getEdgeLabel(b, edgeTime);

              comparison = thisEdgeLabel - thatEdgeLabel;

              if (comparison == 0) {

                // compare to Labels
                int thisToLabel = DFSCodeUtils.getToLabel(a, edgeTime);
                int thatToLabel = DFSCodeUtils.getToLabel(b, edgeTime);

                comparison = thisToLabel - thatToLabel;
              }
            }
          }

          // time differences
        } else {

          // compare backtracking
          boolean thisIsBackwards = thisToTime <= thisFromTime;
          boolean thatIsBackwards = thatToTime <= thatFromTime;

          if (thisIsBackwards && !thatIsBackwards) {
            comparison = -1;
          } else if (thatIsBackwards && !thisIsBackwards) {
            comparison = 1;
          } else {

            // both forwards
            if (thatIsBackwards) {

              // back to earlier vertex is smaller
              comparison = thisToTime - thatToTime;

              // both backwards
            } else {

              // forwards from later vertex is smaller
              comparison = thatFromTime - thisFromTime;
            }
          }
        }

        // stop iteration as soon as one extension differs
        if (comparison != 0) {
          break;
        }
      }

      // one is parent of other
      if (comparison == 0 && !sameSize) {

        // this is child, cause it has further traversals
        if (thisSize > thatSize) {
          comparison = 1;

          // that is child, cause it has further traversals
        } else {
          comparison = -1;

        }
      }
    }

    return comparison;
  }
}
