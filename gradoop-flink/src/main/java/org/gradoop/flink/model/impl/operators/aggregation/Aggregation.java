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
package org.gradoop.flink.model.impl.operators.aggregation;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.operators.aggregation.functions.AggregateEdges;
import org.gradoop.flink.model.impl.operators.aggregation.functions.AggregateVertices;
import org.gradoop.flink.model.impl.operators.aggregation.functions.CombinePartitionAggregates;
import org.gradoop.flink.model.impl.operators.aggregation.functions.SetAggregateProperty;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Takes a logical graph and user defined aggregate functions as input. The
 * aggregate functions are applied on the logical graph and the resulting
 * aggregate is stored as additional properties at the result graph.
 */
public class Aggregation implements UnaryGraphToGraphOperator {

  /**
   * User-defined aggregate functions which are applied on a single logical graph.
   */
  private final Set<AggregateFunction> aggregateFunctions;

  /**
   * Creates new aggregation.
   *
   * @param aggregateFunctions user defined aggregation functions which get
   *                           called on the input graph
   */
  public Aggregation(final AggregateFunction... aggregateFunctions) {
    for (AggregateFunction func : aggregateFunctions) {
      checkNotNull(func);
    }
    this.aggregateFunctions = new HashSet<>(Arrays.asList(aggregateFunctions));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    DataSet<Vertex> vertices = graph.getVertices();
    DataSet<Edge> edges = graph.getEdges();

    DataSet<Map<String, PropertyValue>> aggregate = aggregateVertices(vertices)
      .union(aggregateEdges(edges))
      .reduceGroup(new CombinePartitionAggregates(aggregateFunctions));

    DataSet<GraphHead> graphHead = graph.getGraphHead()
      .map(new SetAggregateProperty(aggregateFunctions))
      .withBroadcastSet(aggregate, SetAggregateProperty.VALUE);

    return graph.getConfig().getLogicalGraphFactory()
      .fromDataSets(graphHead, vertices, edges);
  }

  /**
   * Applies vertex aggregate functions to the partitions of a vertex data set.
   *
   * @param vertices vertex data set
   * @return partition aggregate values mapped from their property key
   */
  private DataSet<Map<String, PropertyValue>> aggregateVertices(DataSet<Vertex> vertices) {
    return vertices.combineGroup(new AggregateVertices(aggregateFunctions.stream()
      .filter(f -> f instanceof VertexAggregateFunction)
      .map(VertexAggregateFunction.class::cast)
      .collect(Collectors.toSet())));
  }

  /**
   * Applies edge aggregate functions to the partitions of an edge data set.
   *
   * @param edges edge data set
   * @return partition aggregate values
   */
  private DataSet<Map<String, PropertyValue>> aggregateEdges(DataSet<Edge> edges) {
    return edges.combineGroup(new AggregateEdges(aggregateFunctions.stream()
      .filter(f -> f instanceof EdgeAggregateFunction)
      .map(EdgeAggregateFunction.class::cast)
      .collect(Collectors.toSet())));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Aggregation.class.getName();
  }
}
