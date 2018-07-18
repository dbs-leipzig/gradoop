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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.functions;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

import java.util.List;

/**
 * Given a set of columns, this key selector returns a concatenated string containing the
 * identifiers of the specified columns.
 *
 * (id0,id1,...,idn),[0,2] -> "id0id2"
 */
public class ExtractJoinColumns implements KeySelector<Embedding, String> {
  /**
   * Columns to concatenate ids from
   */
  private final List<Integer> columns;
  /**
   * Stores the concatenated id string
   */
  private final StringBuilder sb;

  /**
   * Creates the key selector
   *
   * @param columns columns to create hash code from
   */
  public ExtractJoinColumns(List<Integer> columns) {
    this.columns = columns;
    this.sb = new StringBuilder();
  }

  @Override
  public String getKey(Embedding value) throws Exception {
    sb.delete(0, sb.length());
    for (Integer column : columns) {
      sb.append(ArrayUtils.toString(value.getRawId(column)));
    }
    return sb.toString();
  }
}
