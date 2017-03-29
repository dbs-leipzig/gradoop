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

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

/**
 * Input embeddings are extended by an empty path and an additional entry which equals the entry at
 * the expand column. This is done to ensure equally sized embeddings in the case of Expand
 * operations with lower bound 0
 */
public class AdoptEmptyPaths extends RichFlatMapFunction<Embedding, Embedding> {

  /**
   * The column the expansion starts at
   */
  private final int expandColumn;

  /**
   * The column the expanded paths should end at
   */
  private final int closingColumn;

  /**
   * Creates a new UDF instance
   * @param expandColumn column the expantion starts at
   * @param closingColumn column the expanded path should end at
   */
  public AdoptEmptyPaths(int expandColumn, int closingColumn) {
    this.expandColumn = expandColumn;
    this.closingColumn = closingColumn;
  }

  @Override
  public void flatMap(Embedding value, Collector<Embedding> out) throws Exception {
    if (closingColumn >= 0 &&
      !ArrayUtils.isEquals(value.getRawId(expandColumn), value.getRawId(closingColumn))) {
      return;
    }

    value.add();
    value.add(value.getId(expandColumn));
    out.collect(value);
  }
}
