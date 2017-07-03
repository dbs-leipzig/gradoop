
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
