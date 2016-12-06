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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;

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
      sb.append(value.getEntry(column).getId().toString());
    }
    return sb.toString();
  }
}
