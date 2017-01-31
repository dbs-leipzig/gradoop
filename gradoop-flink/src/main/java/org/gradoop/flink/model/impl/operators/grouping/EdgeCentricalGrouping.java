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

package org.gradoop.flink.model.impl.operators.grouping;

import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;

import java.util.List;

/**
 *
 */
public class EdgeCentricalGrouping extends CentricalGrouping{

  public EdgeCentricalGrouping(List<String> primaryGroupingKeys,
    GroupingStrategy groupingStrategy) {
    super(primaryGroupingKeys, groupingStrategy);
  }

  public EdgeCentricalGrouping(List<String> primaryGroupingKeys, List<String> secondaryGroupingKeys,
    GroupingStrategy groupingStrategy) {
    super(primaryGroupingKeys, secondaryGroupingKeys, groupingStrategy);
  }

  public EdgeCentricalGrouping(List<String> primaryGroupingKeys, boolean useVertexLabels,
    List<PropertyValueAggregator> primaryAggregators, List<String> secondaryGroupingKeys,
    boolean useEdgeLabels, List<PropertyValueAggregator> secondaryAggregators) {
    super(primaryGroupingKeys, useVertexLabels, primaryAggregators, secondaryGroupingKeys,
      useEdgeLabels, secondaryAggregators);
  }

  public EdgeCentricalGrouping(List<String> primaryGroupingKeys, boolean useVertexLabels,
    List<PropertyValueAggregator> primaryAggregators, List<String> secondaryGroupingKeys,
    boolean useEdgeLabels, List<PropertyValueAggregator> secondaryAggregators,
    GroupingStrategy groupingStrategy) {
    super(primaryGroupingKeys, useVertexLabels, primaryAggregators, secondaryGroupingKeys,
      useEdgeLabels, secondaryAggregators, groupingStrategy);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Grouping build() {
    return null;
  }

  @Override
  protected LogicalGraph groupReduce(LogicalGraph graph) {
    return null;
  }

  @Override
  protected LogicalGraph groupCombine(LogicalGraph graph) {
    return null;
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return EdgeCentricalGrouping.class.getName() + ":" + getGroupingStrategy();
  }
}
