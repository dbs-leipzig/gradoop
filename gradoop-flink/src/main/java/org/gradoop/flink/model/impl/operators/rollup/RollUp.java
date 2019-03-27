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

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.flink.model.impl.functions.epgm.SetProperty;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;

/**
 * The rollUp operator generates all combinations of the supplied vertex or edge grouping keys
 * according to the definition of the rollUp operation in SQL and uses them together with all
 * opposed grouping keys for separate grouping operations. For example, specifying the grouping
 * keys A, B and C leads to three differently grouped graphs {A,B,C},{A,B},{A} within the resulting
 * graph collection. The grouping can be applied using the vertex or edge grouping keys depending on
 * the implementations of the used sub class.
 */
public abstract class RollUp implements UnaryGraphToCollectionOperator {
  /**
   * Stores grouping keys for vertices.
   */
  protected final List<String> vertexGroupingKeys;

  /**
   * Stores aggregation functions for vertices.
   */
  protected final List<AggregateFunction> vertexAggregateFunctions;

  /**
   * Stores grouping keys for edges.
   */
  protected final List<String> edgeGroupingKeys;

  /**
   * Stores aggregation functions for edges.
   */
  protected final List<AggregateFunction> edgeAggregateFunctions;

  /**
   * Stores the strategy used for grouping.
   */
  protected GroupingStrategy strategy;

  /**
   * Creates a rollUp operator instance with {@link GroupingStrategy#GROUP_REDUCE} as grouping
   * strategy. Use {@link RollUp#setGroupingStrategy(GroupingStrategy)} to define a different
   * grouping strategy.
   *
   * @param vertexGroupingKeys        grouping keys to group vertices
   * @param vertexAggregateFunctions  aggregate functions to apply on super vertices
   * @param edgeGroupingKeys          grouping keys to group edges
   * @param edgeAggregateFunctions    aggregate functions to apply on super edges
   */
  RollUp(
    List<String> vertexGroupingKeys,
    List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingKeys,
    List<AggregateFunction> edgeAggregateFunctions) {
    this.vertexGroupingKeys = vertexGroupingKeys;
    this.vertexAggregateFunctions = vertexAggregateFunctions;
    this.edgeGroupingKeys = edgeGroupingKeys;
    this.edgeAggregateFunctions = edgeAggregateFunctions;
    this.strategy = GroupingStrategy.GROUP_REDUCE;
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
    List<List<String>> groupingKeyCombinations = getGroupingKeyCombinations();

    // for each permutation execute a grouping
    for (List<String> combination : groupingKeyCombinations) {
      // apply the grouping
      LogicalGraph groupedGraph = applyGrouping(graph, combination);

      // add a property to the grouped graph's head to specify the used keys
      PropertyValue groupingKeys = PropertyValue.create(String.join(",", combination));
      DataSet<GraphHead> newGraphHead =
        groupedGraph.getGraphHead().map(new SetProperty<>(getGraphPropertyKey(), groupingKeys));

      if (graphHeads != null && vertices != null && edges != null) {
        // in later iterations union the datasets of the grouped elements with the existing ones
        graphHeads = graphHeads.union(newGraphHead);
        vertices = vertices.union(groupedGraph.getVertices());
        edges = edges.union(groupedGraph.getEdges());
      } else {
        // in the first iteration, fill the datasets
        graphHeads = newGraphHead;
        vertices = groupedGraph.getVertices();
        edges = groupedGraph.getEdges();
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

  /**
   * Creates all combinations of the supplied grouping keys.
   *
   * @param groupingKeys list of all grouping keys to be combined
   * @return list containing all combinations of grouping keys
   */
  List<List<String>> createGroupingKeyCombinations(List<String> groupingKeys) {
    List<List<String>> combinations = new ArrayList<>();
    int elements = groupingKeys.size();

    while (elements > 0) {
      combinations.add(new ArrayList<>(groupingKeys.subList(0, elements)));
      elements--;
    }

    return combinations;
  }

  /**
   * Set the grouping strategy that will be used for each grouping.
   * {@link GroupingStrategy#GROUP_REDUCE} is used as default.
   *
   * @param strategy the strategy to use
   */
  public void setGroupingStrategy(GroupingStrategy strategy) {
    this.strategy = strategy;
  }

  /**
   * Get the property key that is added to each graph head of the grouped graphs inside the
   * resulting collection to specify which property keys are used to group the graph.
   *
   * @return the property key as string
   */
  abstract String getGraphPropertyKey();

  /**
   * Apply the groupBy-operator to the given logical graph and use the given grouping keys as vertex
   * or edge grouping keys (depends on the child class).
   *
   * @param graph the graph the group-By operator is applied on
   * @param groupingKeys the vertex or edge grouping keys to use
   * @return the grouped graph
   */
  abstract LogicalGraph applyGrouping(LogicalGraph graph, List<String> groupingKeys);

  /**
   * Returns all vertex or edge grouping key combinations as list. Internally the
   * {@link RollUp#createGroupingKeyCombinations(List)} function is used to create the combinations.
   * The child class decides, whether the vertex or edge keys are used.
   *
   * @return a list of all vertex or edge grouping key combinations used for rollup grouping
   */
  abstract List<List<String>> getGroupingKeyCombinations();
}
