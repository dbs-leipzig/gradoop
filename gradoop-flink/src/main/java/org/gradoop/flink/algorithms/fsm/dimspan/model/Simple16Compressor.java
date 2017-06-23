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

import me.lemire.integercompression.IntCompressor;
import me.lemire.integercompression.Simple16;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.PatternEmbeddingsMap;

/**
 * Functionality wrapper for Simple16 compression.
 */
public class Simple16Compressor {

  /**
   * Simple16 COMPRESSOR of JavaFastPFOR
   */
  private static final IntCompressor COMPRESSOR = new IntCompressor(new Simple16());

  /**
   * Compression wrapper.
   *
   * @param mux multiplexed graph, pattern or embeddings
   *
   * @return compressed multiplex
   */
  public static int[] compress(int[] mux) {
    return COMPRESSOR.compress(mux);
  }

  /**
   * Decompression wrapper.
   *
   * @param mux compressed multiplex
   * @return multiplexed graph, pattern or embeddings
   */
  public static int[] uncompress(int[] mux) {
    return COMPRESSOR.uncompress(mux);
  }

  /**
   * Convenience method to compress all keys of a pattern-embeddings map
   *
   * @param map pattern-embeddings map
   */
  public static void compressPatterns(PatternEmbeddingsMap map) {
    int[][] patternMuxes = map.getKeys();
    for (int i = 0; i < map.getPatternCount(); i++) {
      patternMuxes[i] = compress(patternMuxes[i]);
    }
  }

  /**
   * Convenience method to compress all values of a pattern-embeddings map
   *
   * @param map pattern-embeddings map
   */
  public static void compressEmbeddings(PatternEmbeddingsMap map) {
    int[][] embeddingMuxes = map.getValues();
    for (int i = 0; i < map.getPatternCount(); i++) {
      embeddingMuxes[i] = compress(embeddingMuxes[i]);
    }
  }
}
