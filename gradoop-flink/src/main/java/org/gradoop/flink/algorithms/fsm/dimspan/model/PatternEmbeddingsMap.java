package org.gradoop.flink.algorithms.fsm.dimspan.model;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * f0 : [pattern1,..,patternN]
 * f1 : [[vertexCount,edgeCount,v11,..,vj1,e11,..,ek1,v21,..]1,..]
 */
public class PatternEmbeddingsMap extends Tuple2<int[][], int[][]> {

  public PatternEmbeddingsMap() {
  }

  public PatternEmbeddingsMap(
    int[][] patternData, int[][] embeddingData) {
    super(patternData, embeddingData);
  }

  public int getPatternCount() {
    return getPatternData().length;
  }

  public int getIndex(int[] pattern) {
    int index = -1;
    int i = 0;
    for (int[] entry : getPatternData()) {
      if(ArrayUtils.isEquals(entry, pattern)) {
        index = i;
        break;
      }
      i++;
    }

    return index;
  }

  public boolean isEmpty() {
    return getPatternCount() == 0;
  }

  public void store(int[] pattern, int[] vertexIds, int[] edgeIds) {
    int patternIndex = getIndex(pattern);
    int[] vertexEdgeIds = ArrayUtils.addAll(vertexIds, edgeIds);

    if (patternIndex < 0) {
      // insert
      setPatternData(ArrayUtils.add(getPatternData(), pattern));

      int[] embeddingData = new int[] {vertexIds.length, edgeIds.length};
      embeddingData = ArrayUtils.addAll(embeddingData, vertexEdgeIds);

      setEmbeddingData(ArrayUtils.add(getEmbeddingData(), embeddingData));
    } else {
      getEmbeddingData()[patternIndex] =
        ArrayUtils.addAll(getEmbeddingData()[patternIndex], vertexEdgeIds);
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    for (int i = 0; i < getPatternCount(); i++) {
      builder.append(getPattern(i));
      builder.append("\t");
      builder.append(new Tuple1<>(getEmbeddingData()[i]));
      builder.append("\n");
    }

    return builder.toString();
  }

  public static PatternEmbeddingsMap getEmptyOne() {
    return new PatternEmbeddingsMap(new int[0][], new int[0][]);
  }

  public void append(PatternEmbeddingsMap that) {
    setPatternData(ArrayUtils.addAll(this.getPatternData(), that.getPatternData()));
    setEmbeddingData(ArrayUtils.addAll(this.getEmbeddingData(), that.getEmbeddingData()));
  }

  public int[] getPattern(int index) {
    return getPatternData()[index];
  }

  public void collect(int[] pattern) {
    setPatternData(ArrayUtils.add(getPatternData(), pattern));
    setEmbeddingData(ArrayUtils.add(getEmbeddingData(), new int[0]));
  }

  public void setPatternData(int[][] patterns) {
    this.f0 = patterns;
  }

  public void setEmbeddingData(int[][] maps) {
    this.f1 = maps;
  }

  public int[][] getPatternData() {
    return this.f0;
  }

  public int[][] getEmbeddingData() {
    return this.f1;
  }

  public int[][] getEmbeddings(int parentIndex, boolean uncompress) {
    int[] embeddingData = getEmbeddingData()[parentIndex];

    if (uncompress) {
      embeddingData = Simple16Compressor.uncompress(embeddingData);
    }

    int vertexCount = embeddingData[0];
    int edgeCount = embeddingData[1];
    int embeddingLength = vertexCount + edgeCount;
    int embeddingCount = (embeddingData.length - 2) / embeddingLength;

    int[][] embeddings = new int[2 * embeddingCount][];

    int i = 2;
    for (int m = 0; m < embeddingCount; m++) {

      int[] vertexIds = new int[vertexCount];
      for (int v = 0; v < vertexCount; v++) {
        vertexIds[v] = embeddingData[i];
        i++;
      }
      embeddings[2*m] = vertexIds;

      int[] edgeIds = new int[edgeCount];
      for (int e = 0; e < edgeCount; e++) {
        edgeIds[e] = embeddingData[i];
        i++;
      }
      embeddings[2*m+1] = edgeIds;
    }

    return embeddings;
  }

  public void put(int[] pattern, int[] embeddingData) {
    setPatternData(ArrayUtils.add(getPatternData(), pattern));
    setEmbeddingData(ArrayUtils.add(getEmbeddingData(), embeddingData));
  }
}
