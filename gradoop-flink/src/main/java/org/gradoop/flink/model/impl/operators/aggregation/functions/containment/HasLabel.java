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

package org.gradoop.flink.model.impl.operators.aggregation.functions.containment;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.bool.Or;

/**
 * superclass of aggregate and filter functions that check vertex or edge label
 * presence in a graph.
 *
 * Usage: First, aggregate and, second, filter using the same UDF instance.
 */
public abstract class HasLabel extends Or
  implements AggregateFunction, FilterFunction<GraphHead> {

  /**
   * label to check presence of
   */
  protected final String label;

  /**
   * Constructor.
   *
   * @param label label to check presence of
   */
  public HasLabel(String label) {
    this.label = label;
  }

  @Override
  public boolean filter(GraphHead graphHead) throws Exception {
    return graphHead.getPropertyValue(getAggregatePropertyKey()).getBoolean();
  }
}
