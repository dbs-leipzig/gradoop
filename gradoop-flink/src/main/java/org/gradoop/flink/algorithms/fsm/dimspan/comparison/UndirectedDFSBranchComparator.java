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
