
package org.gradoop.flink.algorithms.fsm.dimspan.gspan;

import org.gradoop.flink.algorithms.fsm.dimspan.tuples.PatternEmbeddingsMap;

import java.io.Serializable;
import java.util.List;

/**
 * gSpan pattern growth and verification functionality.
 */
public interface GSpanLogic extends Serializable {

  /**
   * Finds all 1-edge patterns and their embeddings in a given graph.
   *
   * @param graph graph
   * @return pattern -> embeddings (k=1)
   */
  PatternEmbeddingsMap getSingleEdgePatternEmbeddings(int[] graph);

  /**
   * Grows children of all supported frequent patterns in a graph.
   *
   * @param graph graph
   * @param parentMap k-edge patter-embedding map
   * @param frequentPatterns k-edge frequent patterns
   * @param rightmostPaths k-edge rightmost paths
   * @param uncompressEmbeddings flag, to enable embedding decompression (true=enabled)
   * @param compressedPatterns compressed k-edge frequent patterns for map lookup
   *
   * @return map of grown supported patterns
   */
  PatternEmbeddingsMap growPatterns(
    int[] graph,
    PatternEmbeddingsMap parentMap,
    List<int[]> frequentPatterns,
    List<int[]> rightmostPaths,
    boolean uncompressEmbeddings,
    List<int[]> compressedPatterns
  );

  /**
   * Verifies if a pattern in DFS-code model
   * is minimal according to gSpan lexicographic order.
   *
   * @param pattern pattern
   * @return true, if minimal
   */
  boolean isMinimal(int[] pattern);

  /**
   * Calculates the rightmost path of a given pattern.
   *
   * @param pattern input pattern
   * @return rightmost path (vertex times)
   */
  int[] getRightmostPathTimes(int[] pattern);

  /**
   * Turns a pattern in DFS code model into a graph
   * @param pattern DFS code
   * @return graph
   */
  int[] getGraph(int[] pattern);
}
