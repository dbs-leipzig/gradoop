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
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;

import java.util.List;

/**
 * Base class for vertex and edge centric grouping.
 */
public abstract class CentricalGrouping extends Grouping {

  /**
   * Grouping strategy.
   */
  private final GroupingStrategy groupingStrategy;

  /**
   * Creates grouping operator instance.
   *
   * @param useVertexLabels   group on vertex label true/false
   * @param useEdgeLabels     group on edge label true/false
   * @param vertexLabelGroups stores grouping properties for vertex labels
   * @param edgeLabelGroups   stores grouping properties for edge labels
   * @param groupingStrategy  grouping strategy
   */
  CentricalGrouping(
    boolean useVertexLabels,
    boolean useEdgeLabels,
    List<LabelGroup> vertexLabelGroups,
    List<LabelGroup> edgeLabelGroups,
    GroupingStrategy groupingStrategy) {

    super(useVertexLabels, useEdgeLabels, vertexLabelGroups, edgeLabelGroups);
    this.groupingStrategy = groupingStrategy;
  }

  /**
   * Overridden by concrete implementations.
   *
   * @param graph input graph
   * @return grouped output graph
   */
  protected abstract LogicalGraph groupReduce(LogicalGraph graph);

  /**
   * Overridden by concrete implementations.
   *
   * @param graph input graph
   * @return grouped output graph
   */
  protected abstract LogicalGraph groupCombine(LogicalGraph graph);

  /**
   * Based on the defined strategy the elements are first combined locally or are directly reduced
   * globally.
   *
   * @param graph input graph
   * @return grouped output graph
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
