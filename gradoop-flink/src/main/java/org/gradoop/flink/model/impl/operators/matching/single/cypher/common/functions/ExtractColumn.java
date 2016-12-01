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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.common.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;

/**
 * Selects the embedding entry specified by index
 * It is also possible to use negative keys, which count from the back of the list.
 * -1 specified the last element, -2 the one before the last.
 */
public class ExtractColumn implements KeySelector<Embedding, GradoopId> {
  /**
   * The specified column index
   */
  private final int column;

  /**
   * Creates a new embedding keys selector
   * @param column index of the embedding entry
   */
  public ExtractColumn(int column) {
    this.column = column;
  }

  @Override
  public GradoopId getKey(Embedding value) throws Exception {
    if (column < 0) {
      return value.getEntry(value.size() + column).getId();
    } else {
      return value.getEntry(column).getId();
    }
  }
}
