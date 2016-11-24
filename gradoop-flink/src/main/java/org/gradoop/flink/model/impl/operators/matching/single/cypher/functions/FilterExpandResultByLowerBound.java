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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdListEntry;

public class FilterExpandResultByLowerBound extends RichFilterFunction<Embedding> {
  private final int minIdListSize;

  public FilterExpandResultByLowerBound(int lowerBound) {
    this.minIdListSize = lowerBound * 2 - 1;
  }

  @Override
  public boolean filter(Embedding value) throws Exception {
    IdListEntry idList = (IdListEntry) value.getEntry(value.size() - 2);
    return idList.getIds().size() >= minIdListSize;
  }
}