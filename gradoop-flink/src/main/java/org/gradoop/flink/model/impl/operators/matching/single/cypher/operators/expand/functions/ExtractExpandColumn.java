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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

/**
 * Extracts a join key from an id stored in an embedding record
 * The id is referenced via its column index.
 */
public class ExtractExpandColumn implements KeySelector<Embedding, GradoopId> {
  /**
   * Column that holds the id which will be used as key
   */
  private final Integer column;

  /**
   * Creates the key selector
   *
   * @param column column that holds the id which will be used as key
   */
  public ExtractExpandColumn(Integer column) {
    this.column = column;
  }

  @Override
  public GradoopId getKey(Embedding value) throws Exception {
    return value.getId(column);
  }
}
