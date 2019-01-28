/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.rollup;

import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;

import java.util.List;

/**
 * Applies the groupBy-operator multiple times on a logical graph using different combinations of
 * the given vertex grouping keys according to the definition of the rollUp operation in SQL.
 *
 * See the description of the abstract class {@link RollUp} for further details.
 */
public class VertexRollUp extends RollUp {

  /**
   * Property key used to store the grouping keys used for rollUp on vertices.
   */
  private static final String VERTEX_GROUPING_KEYS_PROPERTY = "vertexRollUpGroupingKeys";

  /**
   * Creates a vertexRollUp operator instance with {@link GroupingStrategy#GROUP_REDUCE} as grouping
   * strategy. Use {@link RollUp#setGroupingStrategy(GroupingStrategy)} to define a different
   * grouping strategy.
   *
   * @param vertexGroupingKeys grouping keys to group vertices
   * @param vertexAggregateFunctions aggregate functions to apply on super vertices
   * @param edgeGroupingKeys grouping keys to group edges
   * @param edgeAggregateFunctions aggregate functions to apply on super edges
   */
  public VertexRollUp(
    List<String> vertexGroupingKeys,
    List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingKeys,
    List<AggregateFunction> edgeAggregateFunctions
  ) {
    super(vertexGroupingKeys, vertexAggregateFunctions, edgeGroupingKeys, edgeAggregateFunctions);
  }

  @Override
  String getGraphPropertyKey() {
    return VERTEX_GROUPING_KEYS_PROPERTY;
  }

  @Override
  LogicalGraph applyGrouping(LogicalGraph graph, List<String> groupingKeys) {
    return graph.groupBy(groupingKeys, vertexAggregateFunctions, edgeGroupingKeys,
      edgeAggregateFunctions, strategy);
  }

  @Override
  List<List<String>> getGroupingKeyCombinations() {
    return createGroupingKeyCombinations(vertexGroupingKeys);
  }
}
