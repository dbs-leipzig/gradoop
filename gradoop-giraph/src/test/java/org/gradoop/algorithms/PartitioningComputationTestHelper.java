package org.gradoop.algorithms;

/**
 * Base class for all Partitioning related unit tests.
 * Contains a few sample graphs
 * which can be used in specific tests.
 */
public abstract class PartitioningComputationTestHelper {

  /**
   * @return a small graph with two connected partitions
   */
  static String[] getConnectedGraph() {
    return new String[] {
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

  /**
   * @return a small graph with two connected partitions
   */
  static String[] getLPConnectedGraph() {
    return new String[]{
      "0 0 1 2 3",
      "1 1 0 2 3",
      "2 2 0 1 3 4",
      "3 3 0 1 2",
      "4 4 2 5 6 7",
      "5 5 4 6 7 8",
      "6 6 4 5 7",
      "7 7 4 5 6",
      "8 8 5 9 10 11",
      "9 9 8 10 11",
      "10 10 8 9 11",
      "11 11 8 9 10"
    };
  }

  /**
   * @return a small graph with two connected partitions
   */
  static String[] getLPLoopGraph() {
    return new String[]{
      "0 0 0 0 0 0 0 0",
      "0 0 0"
    };
  }

  /**
   * @return a small graph with two disconnected partitions
   */
  static String[] getLPDisconnectedGraph() {
    return new String[]{
      "0 0 1 2 3",
      "1 1 0 2 3",
      "2 2 0 1 3",
      "3 3 0 1 2",
      "4 4 5 6 7",
      "5 5 4 6 7",
      "6 6 4 5 7",
      "7 7 4 5 6"
    };
  }

  /**
   * @return a small bipartite graph
   */
  static String[] getLPBiPartiteGraph() {
    return new String[]{
      "0 0 4",
      "1 1 5",
      "2 2 6",
      "3 3 7",
      "4 4 0",
      "5 5 1",
      "6 6 2",
      "7 7 3"
    };
  }

  /**
   * @return a small graph with two connected partitions
   */
  static String[] getKwaySmallConnectedGraph() {
    return new String[] {
      "0 0 0 1 2 3",
      "1 0 0 0 2 3",
      "2 0 0 0 1 3 4",
      "3 0 0 0 1 2",
      "4 0 0 2 5 6 7",
      "5 0 0 4 6 7",
      "6 0 0 4 5 7",
      "7 0 0 4 5 6"
    };
  }

  /**
   * @return a small bipartite graph
   */
  static String[] getKwayBiPartiteGraph() {
    return new String[] {
      "0 0 0 5 6 7 8 9",
      "1 0 0 5 6 7 8 9",
      "2 0 0 5 6 7 8 9",
      "3 0 0 5 6 7 8 9",
      "4 0 0 5 6 7 8 9",
      "5 0 0 0 1 2 3 4",
      "6 0 0 0 1 2 3 4",
      "7 0 0 0 1 2 3 4",
      "8 0 0 0 1 2 3 4",
      "9 0 0 0 1 2 3 4"
    };
  }



  static String[] getBiPartiteGraph() {
    return new String[] {
      "0 0 4 5 6 7",
      "1 1 4 5 6 7",
      "2 2 4 5 6 7",
      "3 3 4 5 6 7",
      "4 4 0 1 2 3",
      "5 5 0 1 2 3",
      "6 6 0 1 2 3",
      "7 7 0 1 2 3"
    };
  }

}
