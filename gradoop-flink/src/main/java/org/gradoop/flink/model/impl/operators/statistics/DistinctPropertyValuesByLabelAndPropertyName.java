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
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.operators.statistics.functions.CombinePropertyValueDistributionByLabel;
import org.gradoop.flink.model.impl.operators.statistics.functions.ExtractPropertyValuesByLabel;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Base class for Statistic operators calculating the number of distinct property values for
 * label - property name pairs
 *
 * @param <T> element type
 */
public abstract class DistinctPropertyValuesByLabelAndPropertyName<T extends GraphElement>
  implements UnaryGraphToValueOperator<DataSet<WithCount<Tuple2<String, String>>>> {

  /**
   * Calculates number of distinct property values for label - property name pairs of the given
   * elements
   *
   * @param elements graph elements the statistics will be calculated for
   * @return number of distinct property values for each label - property name pair
   */
  DataSet<WithCount<Tuple2<String, String>>> calculate(DataSet<T> elements) {
    return elements
      .flatMap(new ExtractPropertyValuesByLabel<>())
      .groupBy(0)
      .combineGroup(new CombinePropertyValueDistributionByLabel());
  }

}
