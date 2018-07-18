/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
