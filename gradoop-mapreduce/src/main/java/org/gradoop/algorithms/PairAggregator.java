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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.algorithms;

import org.apache.hadoop.hbase.util.Pair;
import org.gradoop.io.formats.GenericPairWritable;

/**
 * Used to aggregate the result in the reduce step of
 * {@link org.gradoop.algorithms.SelectAndAggregate}
 */
public interface PairAggregator {
  /**
   * First element states if the graph fulfills the predicate defined for that
   * job, second element is the aggregated value for that graph.
   *
   * @param values result of map phase in
   *               {@link org.gradoop.algorithms.SelectAndAggregate}
   * @return predicate result and aggregated graph value
   */
  Pair<Boolean, ? extends Number> aggregate(
    Iterable<GenericPairWritable> values);
}
