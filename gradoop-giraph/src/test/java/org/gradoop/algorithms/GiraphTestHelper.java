package org.gradoop.algorithms;

/**
 * Helper class for giraph algorithm tests. Contains sample graphs
 * which can be used in specific tests.
 */
public abstract class GiraphTestHelper {

  /**
   * @return a connected graph where each vertex has its id as value
   */
  static String[] getConnectedGraphWithVertexValues() {
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
   * @return a graph containing a loop with a vertex value
   */
  static String[] getLoopGraphWithVertexValues() {
    return new String[]{
      "0 0 1 2",
      "1 1 0 3",
      "2 1 0 3",
      "3 0 1 2"
    };
  }

  /**
   * @return a small graph with two disconnected partitions
   */
  static String[] getDisconnectedGraphWithVertexValues() {
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
   * Vertices are pair-wise connected.
   *
   * 0 -- 4
   * 1 -- 5
   * 2 -- 6
   * 3 -- 7
   *
   * @return a bipartite graph where each vertex has its id as value
   */
  static String[] getBipartiteGraphWithVertexValues() {
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
   * All vertices are connected.
   *
   * @return a complete bipartite graph where each vertex has its id as value
   */
  static String[] getCompleteBipartiteGraphWithVertexValue() {
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
