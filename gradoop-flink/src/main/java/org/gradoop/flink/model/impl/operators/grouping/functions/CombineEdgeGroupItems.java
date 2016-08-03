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

package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;


import java.util.List;

/**
 * Combines a group of {@link EdgeGroupItem} to a single {@link EdgeGroupItem}.
 */
public class CombineEdgeGroupItems
  extends BuildSuperEdge
  implements GroupCombineFunction<EdgeGroupItem, EdgeGroupItem> {
  /**
   * Creates group reducer
   *
   * @param groupPropertyKeys edge property keys
   * @param useLabel          use edge label
   * @param valueAggregators  aggregate functions for edge values
   */
  public CombineEdgeGroupItems(List<String> groupPropertyKeys, boolean useLabel,
    List<PropertyValueAggregator> valueAggregators) {
    super(groupPropertyKeys, useLabel, valueAggregators);
  }

  /**
   * Reduces edge group items to a single edge group item and collects it.
   *
   * @param edgeGroupItems  edge group items
   * @param collector       output collector
   * @throws Exception
   */
  @Override
  public void combine(Iterable<EdgeGroupItem> edgeGroupItems,
    Collector<EdgeGroupItem> collector) throws Exception {
    collector.collect(reduceInternal(edgeGroupItems));
    resetAggregators();
  }
}
