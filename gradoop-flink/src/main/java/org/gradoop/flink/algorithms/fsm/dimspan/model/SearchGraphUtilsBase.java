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
