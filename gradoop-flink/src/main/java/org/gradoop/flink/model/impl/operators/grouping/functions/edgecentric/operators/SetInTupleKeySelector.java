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

package org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.operators;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;

import java.util.TreeSet;
import java.util.Set;

/**
 * Creates a key of a tuple based on specified fields. To support sets as a key the sets elements
 * are ordered and the ordered elements hash codes are aggregated.
 * @param <T>
 */
public class SetInTupleKeySelector<T extends Tuple> implements KeySelector<T, Long> {

  /**
   * Result hash.
   */
  private Long resultHash;
  /**
   * Ordered set.
   */
  private final TreeSet<Object> sortedSet;
  /**
   * Fields which specify the key.
   */
  private final int[] fields;

  /**
   * Valued constructor.
   *
   * @param fields fields of the tuple which shall be used to generate the key
   */
  public SetInTupleKeySelector(int... fields) {
    this.fields = fields;
    sortedSet = new TreeSet<>();
  }

  @Override
  public Long getKey(T tuple) throws Exception {
    resultHash = 7L;
    for (int i = 0; i < fields.length; i++) {
      if (Set.class.isInstance(tuple.getField(fields[i]))) {
        sortedSet.clear();
        sortedSet.addAll(tuple.getField(fields[i]));
        for (Object object : sortedSet) {
          resultHash = resultHash * 31 + object.hashCode();
        }
      } else {
        resultHash = resultHash * 31 + tuple.getField(fields[i]).hashCode();
      }
    }
    return resultHash;
  }
}
