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

package org.gradoop.flink.algorithms.fsm_old.common.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm_old.common.tuples.SubgraphEmbeddings;

/**
 * Filters embeddings or collected results.
 *
 * @param <SE> subgraph embeddings type
 */
public class IsResult<SE extends SubgraphEmbeddings>
  implements FilterFunction<SE> {

  /**
   * Flag, if embeddings (false) or collected results should be filtered (true).
   */
  private final boolean shouldBeResult;

  /**
   * Constructor.
   *
   * @param shouldBeResult filter mode flag
   */
  public IsResult(boolean shouldBeResult) {
    this.shouldBeResult = shouldBeResult;
  }

  @Override
  public boolean filter(SE embeddings) throws Exception {
    boolean isResult = embeddings.getGraphId().equals(GradoopId.NULL_VALUE);

    return shouldBeResult == isResult;
  }
}
