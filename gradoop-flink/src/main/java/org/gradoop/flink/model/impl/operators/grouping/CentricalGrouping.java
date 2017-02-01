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
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation
  .PropertyValueAggregator;

import java.util.ArrayList;
import java.util.List;


public abstract class CentricalGrouping extends Grouping {

  private final GroupingStrategy groupingStrategy;

//  CentricalGrouping(List<String> primaryGroupingKeys) {
//    this(primaryGroupingKeys, GroupingStrategy.GROUP_REDUCE);
//  }
//
//  CentricalGrouping(List<String> primaryGroupingKeys, GroupingStrategy groupingStrategy) {
//    this(primaryGroupingKeys, new ArrayList<>(), groupingStrategy);
//  }
//
//  CentricalGrouping(List<String> primaryGroupingKeys, List<String> secondaryGroupingKeys) {
//    this(primaryGroupingKeys, secondaryGroupingKeys, GroupingStrategy.GROUP_REDUCE);
//  }
//
//  CentricalGrouping(List<String> primaryGroupingKeys, List<String> secondaryGroupingKeys,
//    GroupingStrategy groupingStrategy) {
//    this(primaryGroupingKeys, false, new ArrayList<>(), secondaryGroupingKeys, false,
//      new ArrayList<>(), groupingStrategy);
//  }

  CentricalGrouping(List<String> primaryGroupingKeys, boolean useVertexLabels,
    List<PropertyValueAggregator> primaryAggregators, List<String> secondaryGroupingKeys,
    boolean useEdgeLabels, List<PropertyValueAggregator> secondaryAggregators,
    GroupingStrategy groupingStrategy) {

    super(primaryGroupingKeys, useVertexLabels, primaryAggregators, secondaryGroupingKeys,
      useEdgeLabels, secondaryAggregators);
    this.groupingStrategy = groupingStrategy;
  }

  //overload build to allow non overloaded groupBy

  public Grouping build(List<String> vertexGroupingKeys) {
    return build(vertexGroupingKeys, null);
  }

  public Grouping build(List<String> vertexGroupingKeys, List<String> edgeGroupingKeys) {
    return build(vertexGroupingKeys, null, edgeGroupingKeys, null, GroupingStrategy.GROUP_REDUCE);
  }

  public Grouping build(
    List<String> vertexGroupingKeys, List<PropertyValueAggregator> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<PropertyValueAggregator> edgeAggregateFunctions,
    GroupingStrategy groupingStrategy) {
    return build(vertexGroupingKeys, false, vertexAggregateFunctions, edgeGroupingKeys, false,
      edgeAggregateFunctions, groupingStrategy);
  }

  public abstract Grouping build(List<String> primaryGroupingKeys, boolean useVertexLabels,
    List<PropertyValueAggregator> primaryAggregators, List<String> secondaryGroupingKeys,
    boolean useEdgeLabels, List<PropertyValueAggregator> secondaryAggregators,
    GroupingStrategy groupingStrategy);

  protected abstract LogicalGraph groupReduce(LogicalGraph graph);

  protected abstract LogicalGraph groupCombine(LogicalGraph graph);

  /**
   * {@inheritDoc}
   */
  @Override
  protected LogicalGraph groupInternal(LogicalGraph graph) {
    switch (getGroupingStrategy()) {
    case GROUP_REDUCE:
      return groupReduce(graph);
    case GROUP_COMBINE:
      return groupCombine(graph);
    default:
      throw new IllegalArgumentException("Unsupported strategy: " + getGroupingStrategy());
    }
  }

  public GroupingStrategy getGroupingStrategy() {
    return groupingStrategy;
  }
}
