package org.gradoop.algorithms;

/**
 * Created by gomezk on 18.12.14.
 */
public abstract class PartitioningComputationTestHelper {

  /**
   * @return a small graph with two connected partitions
   */
  static String[] getConnectedGraph() {
    return new String[]{
      "0 0 1 2 3",
      "1 1 0 2 3",
      "2 2 0 1 3 4",
      "3 3 0 1 2",
      "4 4 2 5 6 7",
      "5 5 4 6 7",
      "6 6 4 5 7",
      "7 7 4 5 6"
    };
  }

  static String[] getSimpleGraph() {
    return new String[] {
      "0 0 1",
      "1 1 0"
    };
  }

}
