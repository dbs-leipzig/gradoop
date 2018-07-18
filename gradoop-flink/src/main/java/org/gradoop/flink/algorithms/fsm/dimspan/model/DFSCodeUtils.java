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
