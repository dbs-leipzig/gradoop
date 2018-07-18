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
package org.gradoop.flink.algorithms.fsm.dimspan.comparison;

import org.gradoop.flink.algorithms.fsm.dimspan.model.DFSCodeUtils;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Comparator for DFS codes based on gSpan lexicographical order.
 * See <a href="https://www.cs.ucsb.edu/~xyan/software/gSpan.htm">gSpan</a>
 */
public class DFSCodeComparator implements Comparator<int[]>, Serializable {

  /**
   * util methods to interpret int-array encoded patterns
   */
  private final DFSCodeUtils dfsCodeUtils = new DFSCodeUtils();

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

      int thisSize = dfsCodeUtils.getEdgeCount(a);
      int thatSize = dfsCodeUtils.getEdgeCount(b);

      boolean sameSize = thisSize == thatSize;

      int minSize  = sameSize || thisSize < thatSize ? thisSize : thatSize;

      // compare extensions
      for (int edgeTime = 0; edgeTime < minSize; edgeTime++) {

        int thisFromTime = dfsCodeUtils.getFromId(a, edgeTime);
        int thatFromTime = dfsCodeUtils.getFromId(b, edgeTime);

        int thisToTime = dfsCodeUtils.getToId(a, edgeTime);
        int thatToTime = dfsCodeUtils.getToId(b, edgeTime);

        // no difference is times
        if (thisFromTime == thatFromTime && thisToTime == thatToTime) {

          // compare from Labels
          int thisFromLabel = dfsCodeUtils.getFromLabel(a, edgeTime);
          int thatFromLabel = dfsCodeUtils.getFromLabel(b, edgeTime);

          comparison = thisFromLabel - thatFromLabel;

          if (comparison == 0) {

            // compare direction
            boolean thisIsOutgoing = dfsCodeUtils.isOutgoing(a, edgeTime);
            boolean thatIsOutgoing = dfsCodeUtils.isOutgoing(b, edgeTime);

            if (thisIsOutgoing && !thatIsOutgoing) {
              comparison = -1;
            } else if (thatIsOutgoing && !thisIsOutgoing) {
              comparison = 1;
            } else {

              // compare edge Labels
              int thisEdgeLabel = dfsCodeUtils.getEdgeLabel(a, edgeTime);
              int thatEdgeLabel = dfsCodeUtils.getEdgeLabel(b, edgeTime);

              comparison = thisEdgeLabel - thatEdgeLabel;

              if (comparison == 0) {

                // compare to Labels
                int thisToLabel = dfsCodeUtils.getToLabel(a, edgeTime);
                int thatToLabel = dfsCodeUtils.getToLabel(b, edgeTime);

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
