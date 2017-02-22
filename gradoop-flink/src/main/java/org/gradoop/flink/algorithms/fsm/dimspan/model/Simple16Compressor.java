package org.gradoop.flink.algorithms.fsm.dimspan.model;


import me.lemire.integercompression.IntCompressor;
import me.lemire.integercompression.Simple16;

public class Simple16Compressor {

  private static final IntCompressor compressor = new IntCompressor(new Simple16());

  public static int[] compress(int[] ints) {
    return compressor.compress(ints);
  }

  public static int[] uncompress(int[] ints) {
    return compressor.uncompress(ints);
  }

  public static void compressEmbeddings(PatternEmbeddingsMap map) {
    int[][] embeddingData = map.getEmbeddingData();
    for (int i = 0; i < map.getPatternCount(); i++ ) {
      embeddingData[i] = compress(embeddingData[i]);
    }
  }

  public static void compressPatterns(PatternEmbeddingsMap map) {
    int[][] patternData = map.getPatternData();
    for (int i = 0; i < map.getPatternCount(); i++ ) {
      patternData[i] = compress(patternData[i]);
    }
  }

  public static void uncompressPatterns(PatternEmbeddingsMap map) {
    int[][] patternData = map.getPatternData();
    for (int i = 0; i < map.getPatternCount(); i++ ) {
      patternData[i] = uncompress(patternData[i]);
    }
  }
}
