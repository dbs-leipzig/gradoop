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

import org.apache.commons.lang3.ArrayUtils;

/**
 * Util methods to interpret and manipulate int-array encoded patterns represented by DFS codes.
 */
public class DFSCodeUtils extends GraphUtilsBase {

  /**
   * Extracts the branch of a give DFS code multiplex.
   *
   * @param mux DFS code multiplex
   *
   * @return mux of its first extension (branch)
   */
  public int[] getBranch(int[] mux) {
    return ArrayUtils.subarray(mux, 0, EDGE_LENGTH);
  }

  /**
   * Extends a parent DFS code's multiplex
   *
   * @param parentMux multiplex
   * @param fromTime from time
   * @param fromLabel from label
   * @param outgoing outgoing traversal
   * @param edgeLabel traversed edge label
   * @param toTime to time
   * @param toLabel to label
   *
   * @return child DFS code's multiplex
   */
  public int[] addExtension(int[] parentMux,
    int fromTime, int fromLabel, boolean outgoing, int edgeLabel, int toTime, int toLabel) {

    return ArrayUtils.addAll(
      parentMux, multiplex(fromTime, fromLabel, outgoing, edgeLabel, toTime, toLabel));
  }

  /**
   * Checks if a DFS code is child of another one.
   *
   * @param childMux multiplex of the child's DFS code
   * @param parentMux mulitplex of the parent't DFS code
   *
   * @return true, if child is actually a child of the parent
   */
  public boolean isChildOf(int[] childMux, int[] parentMux) {
    boolean isChild = parentMux.length >= childMux.length;

    if (isChild) {
      for (int i = 0; i < childMux.length; i++) {
        if (childMux[i] != parentMux[i]) {
          isChild = false;
          break;
        }
      }
    }

    return isChild;
  }
}
