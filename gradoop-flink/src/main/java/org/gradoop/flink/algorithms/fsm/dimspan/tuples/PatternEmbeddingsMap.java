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
