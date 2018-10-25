/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.flink.model.impl.functions.epgm.SetLabelAndProperty;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;

/**
 * The rollUp operator generates all combinations of the supplied vertex or edge grouping keys
 * according to the definition of the rollUp operation in SQL and uses them together with all
 * opposed grouping keys for separate grouping operations. For example, specifying the grouping
 * keys A, B and C leads to three differently grouped graphs {A,B,C},{A,B},{A} within the resulting
 * graph collection.
 */
public class RollUp implements UnaryGraphToCollectionOperator {

  /**
   * Used to distinguish between a rollUp on vertices or a rollUp on edges.
   */
  public enum RollUpType {

    /**
     * Used for rollUp on vertex grouping keys.
     */
    VERTEX_ROLLUP,

    /**
     * Used for rollUp on edge grouping keys.
     */
    EDGE_ROLLUP
  }

  /**
   * Property key used to store the grouping keys used for rollUp on vertices.
   */
  public static final String VERTEX_GROUPING_KEYS_PROPERTY = "vertexRollUpGroupingKeys";

  /**
   * Property key used to store the grouping keys used for rollUp on edges.
   */
  public static final String EDGE_GROUPING_KEYS_PROPERTY = "edgeRollUpGroupingKeys";

  /**
   * Stores grouping keys for vertices.
   */
  private final List<String> vertexGroupingKeys;

  /**
   * Stores aggregation functions for vertices.
   */
  private final List<PropertyValueAggregator> vertexAggregateFunctions;

  /**
   * Stores grouping keys for edges.
   */
  private final List<String> edgeGroupingKeys;

  /**
   * Stores aggregation functions for edges.
   */
  private final List<PropertyValueAggregator> edgeAggregateFunctions;

  /**
   * Stores the type of rollUp to be executed.
   */
  private final RollUpType rollUpType;

  /**
   * Stores the strategy used for grouping.
   */
  private final GroupingStrategy strategy;

  /**
   * Creates a rollUp operator instance with {@link GroupingStrategy#GROUP_REDUCE} as grouping
   * strategy.
   *
   * @param vertexGroupingKeys        grouping keys to group vertices
   * @param vertexAggregateFunctions  aggregate functions to apply on super vertices
   * @param edgeGroupingKeys          grouping keys to group edges
   * @param edgeAggregateFunctions    aggregate functions to apply on super edges
   * @param rollUpType                type of rollUp to be executed
   */
  public RollUp(
    List<String> vertexGroupingKeys,
    List<PropertyValueAggregator> vertexAggregateFunctions,
    List<String> edgeGroupingKeys,
    List<PropertyValueAggregator> edgeAggregateFunctions,
    RollUpType rollUpType) {
    this(
      vertexGroupingKeys,
      vertexAggregateFunctions,
      edgeGroupingKeys,
      edgeAggregateFunctions,
      rollUpType,
      GroupingStrategy.GROUP_REDUCE);
  }

  /**
   * Creates rollUp operator instance.
   *
   * @param vertexGroupingKeys       grouping keys to group vertices
   * @param vertexAggregateFunctions aggregate functions to apply on super vertices
   * @param edgeGroupingKeys         grouping keys to group edges
   * @param edgeAggregateFunctions   aggregate functions to apply on super edges
   * @param rollUpType               type of rollUp to be executed
   * @param strategy                 strategy used for grouping
   */
  public RollUp(
    List<String> vertexGroupingKeys,
    List<PropertyValueAggregator> vertexAggregateFunctions,
    List<String> edgeGroupingKeys,
    List<PropertyValueAggregator> edgeAggregateFunctions,
    RollUpType rollUpType,
    GroupingStrategy strategy) {
    this.vertexGroupingKeys = vertexGroupingKeys;
    this.vertexAggregateFunctions = vertexAggregateFunctions;
    this.edgeGroupingKeys = edgeGroupingKeys;
    this.edgeAggregateFunctions = edgeAggregateFunctions;
    this.rollUpType = rollUpType;
    this.strategy = strategy;
  }

  /**
   * Creates all combinations of the supplied grouping keys.
   *
   * @param groupingKeys list of all grouping keys to be combined
   * @return list containing all combinations of grouping keys
   */
  private List<List<String>> createGroupingKeyCombinations(List<String> groupingKeys) {
    List<List<String>> combinations = new ArrayList<>();

    while (!groupingKeys.isEmpty()) {
      combinations.add(new ArrayList<>(groupingKeys));

      groupingKeys.remove(groupingKeys.size() - 1);
    }

    return combinations;
  }

  /**
   * Applies the rollUp operation on the given input graph.
   *
   * @param graph input graph
   * @return graphCollection containing all differently grouped graphs
   */
  @Override
  public GraphCollection execute(LogicalGraph graph) {
    DataSet<GraphHead> graphHeads = null;
    DataSet<Vertex> vertices = null;
    DataSet<Edge> edges = null;

    if (rollUpType == RollUpType.VERTEX_ROLLUP) {
      List<List<String>> vertexGKCombinations = createGroupingKeyCombinations(vertexGroupingKeys);

      for (int c = 0; c <= vertexGKCombinations.size() - 1; c++) {
        List<String> combination = vertexGKCombinations.get(c);
        String newGraphHeadLabel = "g" + c;
        LogicalGraph groupedGraph = graph.groupBy(combination, vertexAggregateFunctions,
          edgeGroupingKeys, edgeAggregateFunctions, strategy);

        PropertyValue groupingKeys = PropertyValue.create(String.join(",", combination));
        DataSet<GraphHead> newGraphHead = groupedGraph.getGraphHead().map(
          new SetLabelAndProperty<>(
            newGraphHeadLabel, VERTEX_GROUPING_KEYS_PROPERTY, groupingKeys));

        if (graphHeads == null && vertices == null && edges == null) {
          graphHeads = newGraphHead;
          vertices = groupedGraph.getVertices();
          edges = groupedGraph.getEdges();
        } else if (graphHeads != null && vertices != null && edges != null) {
          graphHeads = graphHeads.union(newGraphHead);
          vertices = vertices.union(groupedGraph.getVertices());
          edges = edges.union(groupedGraph.getEdges());
        }
      }
    } else if (rollUpType == RollUpType.EDGE_ROLLUP) {
      List<List<String>> edgeGKCombinations = createGroupingKeyCombinations(edgeGroupingKeys);

      for (int c = 0; c <= edgeGKCombinations.size() - 1; c++) {
        List<String> combination = edgeGKCombinations.get(c);
        String newGraphHeadLabel = "g" + c;
        LogicalGraph groupedGraph = graph.groupBy(vertexGroupingKeys, vertexAggregateFunctions,
          combination, edgeAggregateFunctions, strategy);

        PropertyValue groupingKeys = PropertyValue.create(String.join(",", combination));
        DataSet<GraphHead> newGraphHead = groupedGraph.getGraphHead().map(
          new SetLabelAndProperty<>(
            newGraphHeadLabel, EDGE_GROUPING_KEYS_PROPERTY, groupingKeys));

        if (graphHeads == null && vertices == null && edges == null) {
          graphHeads = newGraphHead;
          vertices = groupedGraph.getVertices();
          edges = groupedGraph.getEdges();
        } else if (graphHeads != null && vertices != null && edges != null) {
          graphHeads = graphHeads.union(newGraphHead);
          vertices = vertices.union(groupedGraph.getVertices());
          edges = edges.union(groupedGraph.getEdges());
        }
      }
    }

    // We initialized the DataSets with null, so it may be possible that they're still null here,
    // so we should check and return an empty collection in this case.
    // But the overhead of creating an empty collection should only be done, if at least one of the
    // DataSets is null.
    GraphCollection collection;
    if (graphHeads != null && vertices != null && edges != null) {
      collection = graph.getConfig().getGraphCollectionFactory()
        .fromDataSets(graphHeads, vertices, edges);
    } else {
      collection = graph.getConfig().getGraphCollectionFactory().createEmptyCollection();
    }

    return collection;
  }

  @Override
  public String getName() {
    return RollUp.class.getName();
  }
}
