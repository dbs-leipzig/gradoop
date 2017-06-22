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
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;

import java.util.ArrayList;
import java.util.List;


public abstract class CentricalGrouping extends Grouping {

  private final GroupingStrategy groupingStrategy;

  CentricalGrouping(
    boolean useVertexLabels,
    boolean useEdgeLabels,
    List<LabelGroup> vertexLabelGroups,
    List<LabelGroup> edgeLabelGroups,
    GroupingStrategy groupingStrategy) {

    super(useVertexLabels, useEdgeLabels, vertexLabelGroups, edgeLabelGroups);
    this.groupingStrategy = groupingStrategy;
  }

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
