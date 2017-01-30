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
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.operators.statistics.functions.CombinePropertyValueDistribution;
import org.gradoop.flink.model.impl.operators.statistics.functions.ExtractPropertyValues;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Base class for Statistic operators calculating the number of distinct property values for
 * label - property name pairs
 *
 * @param <T> element type
 */
public abstract class DistinctPropertyValuesByPropertyName<T extends GraphElement>
  implements UnaryGraphToValueOperator<DataSet<WithCount<String>>> {

  /**
   * Calculates number of distinct property values for label - property name pairs of the given
   * elements
   *
   * @param elements graph elements the statistics will be calculated for
   * @return number of distinct property values for each label - property name pair
   */
  DataSet<WithCount<String>> calculate(DataSet<T> elements) {
    return elements
      .flatMap(new ExtractPropertyValues<>())
      .groupBy(0)
      .combineGroup(new CombinePropertyValueDistribution());
  }

}
