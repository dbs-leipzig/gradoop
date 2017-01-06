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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.tuple.Tuple2ToWithCount;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Base class to compute value distributions of computed from EPGM elements.
 *
 * @param <EL> EPGM element type
 * @param <T> value type
 */
abstract class ValueDistribution<EL extends Element, T>
  implements UnaryGraphToValueOperator<DataSet<WithCount<T>>> {

  /**
   * Maps an EPGM element to a value that can be counted.
   */
  private final MapFunction<EL, T> valueFunction;

  /**
   * Constructor
   *
   * @param valueFunction extracts a value from an EPGM element
   */
  ValueDistribution(MapFunction<EL, T> valueFunction) {
    this.valueFunction = valueFunction;
  }

  /**
   * Maps the EPGM element to a value according to the specified {@code valueFunction} and groups
   * those values and counts the values per group.
   *
   * @param elements EPGM elements
   * @return extracted values and the corresponding number of elements with that value
   */
  DataSet<WithCount<T>> compute(DataSet<EL> elements) {
    return Count.groupBy(elements.map(valueFunction))
      .map(new Tuple2ToWithCount<>());
  }
}
