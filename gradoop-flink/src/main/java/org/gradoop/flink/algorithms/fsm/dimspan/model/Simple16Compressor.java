
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
