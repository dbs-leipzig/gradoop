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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandIntermediateResult;

/**
 * Filters expand results that are shorter than the lower bound
 */
public class FilterExpandResultByLowerBound extends RichFilterFunction<ExpandIntermediateResult> {
  /**
   * The minimum path size
   */
  private final int minPathSize;

  /**
   * New Filter operator
   * @param lowerBound lower bound path length
   */
  public FilterExpandResultByLowerBound(int lowerBound) {
    this.minPathSize = lowerBound * 2 - 1;
  }

  @Override
  public boolean filter(ExpandIntermediateResult value) throws Exception {
    return value.pathSize() >= minPathSize;
  }
}
