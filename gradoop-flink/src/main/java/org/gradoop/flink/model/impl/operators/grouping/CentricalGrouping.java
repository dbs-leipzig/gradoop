/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
