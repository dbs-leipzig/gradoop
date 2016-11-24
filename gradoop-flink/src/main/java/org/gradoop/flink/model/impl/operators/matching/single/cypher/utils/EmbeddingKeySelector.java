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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.utils;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;

public class EmbeddingKeySelector implements KeySelector<Embedding, GradoopId> {
  public static final int LAST = -1;

  public final int column;

  public EmbeddingKeySelector(int column) {
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
