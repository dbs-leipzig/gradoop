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

package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.statistics.functions.CombinePropertyValueDistribution;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Set;

/**
 * Base class for Statistic operators calculating the number of distinct property values grouped by
 * a given key K
 *
 * @param <T> element type
 * @param <K> grouping key
 */
public abstract class DistinctProperties<T extends GraphElement, K>
  implements UnaryGraphToValueOperator<DataSet<WithCount<K>>> {


  @Override
  public DataSet<WithCount<K>> execute(LogicalGraph graph) {
    return extractValuePairs(graph)
      .groupBy(0)
      .reduceGroup(new CombinePropertyValueDistribution<>());
  }

  /**
   * Extracts key value pairs from the given logical graph
   * @param graph input graph
   * @return key value pairs
   */
  protected abstract DataSet<Tuple2<K, Set<PropertyValue>>> extractValuePairs(LogicalGraph graph);
}
