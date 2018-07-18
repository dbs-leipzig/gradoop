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
 * Util methods to interpret and manipulate int-array encoded graphs
 */
public abstract class SearchGraphUtilsBase extends GraphUtilsBase implements SearchGraphUtils {

  @Override
  public int[] addEdge(
    int[] graphMux, int sourceId, int sourceLabel, int edgeLabel, int targetId, int targetLabel) {

    int[] edgeMux;

    // determine minimum 1-edge DFS code
    if (sourceLabel <= targetLabel) {
      edgeMux = multiplex(sourceId, sourceLabel, true, edgeLabel, targetId, targetLabel);
    } else {
      edgeMux = multiplex(targetId, targetLabel, false, edgeLabel, sourceId, sourceLabel);
    }

    graphMux = ArrayUtils.addAll(graphMux, edgeMux);

    return graphMux;
  }
}
