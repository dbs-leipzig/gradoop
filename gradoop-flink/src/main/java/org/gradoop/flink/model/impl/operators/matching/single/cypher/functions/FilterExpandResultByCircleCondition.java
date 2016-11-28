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

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandIntermediateResult;

/**
 * Filters ExpandResults with the given circle pattern
 */
public class FilterExpandResultByCircleCondition implements
  FilterFunction<ExpandIntermediateResult> {

  /**
   * defines the column which should be equal with the paths end
   */
  private final int circle;

  /**
   * Create new filter
   * @param circle defines the column which should be equal with the paths end
   */
  public FilterExpandResultByCircleCondition(int circle) {
    this.circle = circle;
  }

  @Override
  public boolean filter(ExpandIntermediateResult value) throws Exception {
    return value.getBase().getEntry(circle).contains(value.getEnd());
  }
}
