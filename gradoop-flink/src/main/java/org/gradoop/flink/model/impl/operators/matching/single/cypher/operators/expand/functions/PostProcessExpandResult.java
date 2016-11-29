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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.ExpandIntermediateResult;

/**
 * Postprocesses the expand iteration results
 * 1. Remove paths below lower bound length
 * 2. Remove results that do not match circle condition
 * 3. Turn intermediate results into embeddings
 */
public class PostProcessExpandResult
  extends RichFlatMapFunction<ExpandIntermediateResult, Embedding> {

  /**
   * Holds the minimum path size calculated from lower bound
   */
  private final int minPathLength;
  /**
   * Specifies the base column which should be equal to the paths end column
   */
  private final int closingColumn;

  /**
   * Create a new Postprocess function
   *
   * @param lowerBound the lower bound path length
   * @param closingColumn the base column which should equal the paths end column
   */
  public PostProcessExpandResult(int lowerBound, int closingColumn) {
    this.minPathLength = lowerBound * 2 - 1;
    this.closingColumn = closingColumn;
  }

  @Override
  public void flatMap(ExpandIntermediateResult value, Collector<Embedding> out) throws Exception {
    if (value.pathSize() < minPathLength) {
      return;
    }

    if (closingColumn >= 0 && !value.getBase().getEntry(closingColumn).contains(value.getEnd())) {
      return;
    }

    out.collect(value.toEmbedding());
  }
}
