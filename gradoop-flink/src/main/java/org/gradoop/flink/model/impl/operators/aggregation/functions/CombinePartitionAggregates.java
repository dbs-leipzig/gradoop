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

package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.Iterator;

/**
 * partitionAggregateValue,.. => globalAggregateValue
 */
public class CombinePartitionAggregates
  implements GroupReduceFunction<PropertyValue, PropertyValue> {

  /**
   * Aggregate Function
   */
  private final AggregateFunction aggregateFunction;

  /**
   * Constructor.
   *
   * @param aggregateFunction aggregate function
   */
  public CombinePartitionAggregates(AggregateFunction aggregateFunction) {
    this.aggregateFunction = aggregateFunction;
  }

  @Override
  public void reduce(Iterable<PropertyValue> partitionAggregates,
    Collector<PropertyValue> out) throws Exception {

    Iterator<PropertyValue> iterator = partitionAggregates.iterator();

    PropertyValue aggregate = iterator.next();

    while (iterator.hasNext()) {
      aggregateFunction.aggregate(aggregate, iterator.next());
    }

    out.collect(aggregate);
  }
}
