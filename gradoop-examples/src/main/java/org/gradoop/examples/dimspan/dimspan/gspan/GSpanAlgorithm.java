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

package org.gradoop.examples.dimspan.dimspan.gspan;

import org.gradoop.examples.dimspan.dimspan.representation.PatternEmbeddingsMap;

import java.io.Serializable;
import java.util.List;

/**
 * gSpan pattern growth and verification functionality.
 */
public interface GSpanAlgorithm extends Serializable {

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
   * @param compressedPatterns compressed k-edge frequent patterns for map lookup
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
   * Verifies if a pattern in DFS-code representation
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
   * Turns a pattern in DFS code representation into a graph
   * @param pattern DFS code
   * @return graph
   */
  int[] getGraph(int[] pattern);
}
