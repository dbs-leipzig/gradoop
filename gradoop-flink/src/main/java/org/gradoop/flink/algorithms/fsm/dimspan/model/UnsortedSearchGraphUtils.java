
package org.gradoop.flink.algorithms.fsm.dimspan.model;

/**
 * Util methods to interpret and manipulate unsorted int-array encoded graphs
 */
public class UnsortedSearchGraphUtils extends SearchGraphUtilsBase implements SearchGraphUtils {

  @Override
  public int getFirstGeqEdgeId(int[] graphMux, int[] searchMux, int searchFromEdgeId) {
    return 0;
  }

}
