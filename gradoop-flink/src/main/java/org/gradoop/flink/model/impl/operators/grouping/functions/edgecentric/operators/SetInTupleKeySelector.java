/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
