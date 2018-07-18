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
package org.gradoop.flink.algorithms.fsm.dimspan.tuples;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.algorithms.fsm.dimspan.model.Simple16Compressor;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Objects;

/**
 * f0 : [pattern1,..,patternN]
 * f1 : [[vertexCount,edgeCount,v11,..,vj1,e11,..,ek1,v21,..]1,..]
 */
public class PatternEmbeddingsMap extends Tuple2<int[][], int[][]> {

  /**
   * Default Constructor.
   */
  public PatternEmbeddingsMap() {
  }

  /**
   * Valued constructor.
   *
   * @param patternMuxes multiplexes of patterns
   * @param embeddingMuxes multiplexes of embeddings
   */
  public PatternEmbeddingsMap(
    int[][] patternMuxes, int[][] embeddingMuxes) {
    super(patternMuxes, embeddingMuxes);
  }

  /**
   * Convenience method.
   *
   * @return number of contained patterns
   */
  public int getPatternCount() {
    return getKeys().length;
  }


  /**
   * Convenience method to check is an embeddings map is empty
   *
   * @return true, if empty
   */
  public boolean isEmpty() {
    return getPatternCount() == 0;
  }

  /**
   * Returns a pattern at a given index.
   *
   * @param index index
   *
   * @return pattern multiplex
   */
  public int[] getPattern(int index) {
    return getKeys()[index];
  }

  /**
   * Find index of a given pattern
   *
   * @param  patternMux search pattern
   *
   * @return index if contained, -1 otherwise
   */
  public int getIndex(int[] patternMux) {
    int index = -1;
    int i = 0;
    for (int[] mux : getKeys()) {
      if (Objects.deepEquals(mux, patternMux)) {
        index = i;
        break;
      }
      i++;
    }

    return index;
  }

  /**
   * Creates an entry for a pattern including an initial embedding or just adds an embedding.
   *
   * @param patternMux pattern
   * @param vertexIds the embedding's vertex ids
   * @param edgeIds the embedding's edge ids
   */
  public void put(int[] patternMux, int[] vertexIds, int[] edgeIds) {
    int patternIndex = getIndex(patternMux);
    int[] vertexEdgeIds = ArrayUtils.addAll(vertexIds, edgeIds);

    if (patternIndex < 0) {
      // insert
      setKeys(ArrayUtils.add(getKeys(), patternMux));

      int[] embeddingData = new int[] {vertexIds.length, edgeIds.length};
      embeddingData = ArrayUtils.addAll(embeddingData, vertexEdgeIds);

      setValues(ArrayUtils.add(getValues(), embeddingData));
    } else {
      // update
      getValues()[patternIndex] =
        ArrayUtils.addAll(getValues()[patternIndex], vertexEdgeIds);
    }
  }

  /**
   * Adds a pattern and existing embeddings.
   *
   * @param patternMux pattern
   * @param embeddingsMux embeddings
   */
  public void put(int[] patternMux, int[] embeddingsMux) {
    setKeys(ArrayUtils.add(getKeys(), patternMux));
    setValues(ArrayUtils.add(getValues(), embeddingsMux));
  }

  /**
   * Adds all patterns and embeddings of another map.
   *
   * @param that other map
   */
  public void append(PatternEmbeddingsMap that) {
    setKeys(ArrayUtils.addAll(this.getKeys(), that.getKeys()));
    setValues(ArrayUtils.addAll(this.getValues(), that.getValues()));
  }


  /**
   * Adds a pattern but no embeddings.
   *
   * @param muxWithCount pattern
   */
  public void collect(WithCount<int[]> muxWithCount) {
    setKeys(ArrayUtils.add(getKeys(), muxWithCount.getObject()));
    setValues(ArrayUtils.add(getValues(), new int[] {(int) muxWithCount.getCount()}));
  }

  /**
   * Returns all embeddings for a give pattern index.
   *
   * @param index pattern index
   * @param uncompress true, if embeddings need to be uncompressed
   *
   * @return array of embeddings
   */
  public int[][] getEmbeddings(int index, boolean uncompress) {
    int[] embeddingData = getValues()[index];

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
      embeddings[2 * m] = vertexIds;

      int[] edgeIds = new int[edgeCount];
      for (int e = 0; e < edgeCount; e++) {
        edgeIds[e] = embeddingData[i];
        i++;
      }
      embeddings[2 * m + 1] = edgeIds;
    }

    return embeddings;
  }

  // GETTERS AND SETTERS

  public int[][] getKeys() {
    return this.f0;
  }

  public int[][] getValues() {
    return this.f1;
  }

  private void setKeys(int[][] patternMuxes) {
    this.f0 = patternMuxes;
  }

  private void setValues(int[][] embeddingMuxes) {
    this.f1 = embeddingMuxes;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    for (int i = 0; i < getPatternCount(); i++) {
      builder.append(new Tuple1<>(getPattern(i)));
      builder.append("\t");
      builder.append(new Tuple1<>(getValues()[i]));
      builder.append("\n");
    }

    return builder.toString();
  }

  /**
   * Convenience method to create and empty map.
   *
   * @return empty map
   */
  public static PatternEmbeddingsMap getEmptyOne() {
    return new PatternEmbeddingsMap(new int[0][], new int[0][]);
  }
}
