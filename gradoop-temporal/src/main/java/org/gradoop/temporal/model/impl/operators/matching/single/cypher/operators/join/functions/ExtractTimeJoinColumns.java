/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.functions;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

import java.util.List;

/**
 * Given a set of time selectors, this key selector returns a concatenated string containing the
 * specified time values
 * Time Selectors here are of the form (a,b) where a denotes the time column and 0<=b<=3 the
 * time value to select: 0=tx_from, 1=tx_to, 2=valid_from, 3=valid_to
 * <p>
 * {@code ([1L,2L,3L,4L]),[(0,2),(0,3)] -> "3L4L"}
 */
public class ExtractTimeJoinColumns implements KeySelector<EmbeddingTPGM, String> {
  /**
   * Time selectors to concatenate
   * Time Selectors are of the form (a,b) where a denotes the time column and 0<=b<=3 the
   * time value to select: 0=tx_from, 1=tx_to, 2=valid_from, 3=valid_to
   */
  private final List<Tuple2<Integer, Integer>> timeData;
  /**
   * Stores the concatenated key string
   */
  private final StringBuilder sb;

  /**
   * used to separate different values in their concatenation
   */
  private static final String sep = "|";

  /**
   * Creates the key selector
   *
   * @param timeData timedata to create hash code from
   */
  public ExtractTimeJoinColumns(List<Tuple2<Integer, Integer>> timeData) {
    this.timeData = timeData;
    this.sb = new StringBuilder();
  }

  @Override
  public String getKey(EmbeddingTPGM value) throws Exception {
    sb.delete(0, sb.length());
    for (Tuple2<Integer, Integer> selector : timeData) {
      sb.append(ArrayUtils.toString(fetchTime(value, selector))).append(sep);
    }
    return sb.toString();
  }

  /**
   * Retrieves the time value denoted by a time selector
   *
   * @param embedding the embedding to look up the time value in
   * @param selector  the selector denoting the time value. Selectors are of the form
   *                  (a,b) where a denotes the time column and 0<=b<=3 the time value to select:
   *                  0=tx_from, 1=tx_to, 2=valid_from, 3=valid_to
   * @return the time value denoted by the selector
   * @throws IllegalArgumentException if b in selector (a,b) not in [0,3]
   */
  private Long fetchTime(EmbeddingTPGM embedding, Tuple2<Integer, Integer> selector) {
    try {
      return embedding.getTimes(selector.f0)[selector.f1];
    } catch (IndexOutOfBoundsException e) {
      throw new IllegalArgumentException("invalid time selector");
    }
  }
}
