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
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.api.operators.ApplicableUnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.ElementsOfSelectedGraphs;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.AggregateTransactions;
import org.gradoop.flink.model.impl.operators.aggregation.functions.ApplyAggregateEdges;
import org.gradoop.flink.model.impl.operators.aggregation.functions.ApplyAggregateVertices;
import org.gradoop.flink.model.impl.operators.aggregation.functions.CombinePartitionApplyAggregates;
import org.gradoop.flink.model.impl.operators.aggregation.functions.SetAggregateProperties;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Takes a collection of logical graphs and user defined aggregate functions as
 * input. The aggregate functions are applied on each logical graph contained in
 * the collection and the aggregate is stored as additional properties at the graphs.
 */
public class ApplyAggregation
  implements ApplicableUnaryGraphToGraphOperator {

  /**
   * User-defined aggregate functions which get applied on a graph collection.
   */
  private final Set<AggregateFunction> aggregateFunctions;

  /**
   * Creates a new operator instance.
   *
   * @param aggregateFunctions functions to compute aggregate values
   */
  public ApplyAggregation(final AggregateFunction... aggregateFunctions) {
    for (AggregateFunction aggFunc : aggregateFunctions) {
      checkNotNull(aggFunc);
    }
    this.aggregateFunctions = new HashSet<>(Arrays.asList(aggregateFunctions));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection executeForGVELayout(GraphCollection collection) {
    DataSet<GraphHead> graphHeads = collection.getGraphHeads();
    DataSet<GradoopId> graphIds = graphHeads.map(new Id<>());

    DataSet<Tuple2<GradoopId, Map<String, PropertyValue>>> aggregate =
      aggregateVertices(collection.getVertices(), graphIds)
        .union(aggregateEdges(collection.getEdges(), graphIds))
        .groupBy(0)
        .reduceGroup(new CombinePartitionApplyAggregates(aggregateFunctions));

    graphHeads = graphHeads
      .coGroup(aggregate)
      .where(new Id<>()).equalTo(0)
      .with(new SetAggregateProperties(aggregateFunctions));

    return collection.getConfig().getGraphCollectionFactory()
      .fromDataSets(graphHeads, collection.getVertices(), collection.getEdges());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection executeForTxLayout(GraphCollection collection) {
    DataSet<GraphTransaction> updatedTransactions = collection.getGraphTransactions()
      .map(new AggregateTransactions(aggregateFunctions));

    return collection.getConfig().getGraphCollectionFactory().fromTransactions(updatedTransactions);
  }

  /**
   * Applies an aggregate function to the partitions of a vertex data set.
   *
   * @param vertices vertex data set
   * @param graphIds graph ids to aggregate
   * @return partition aggregate value
   */
  private DataSet<Tuple2<GradoopId, Map<String, PropertyValue>>> aggregateVertices(
    DataSet<Vertex> vertices, DataSet<GradoopId> graphIds) {
    return vertices
      .flatMap(new ElementsOfSelectedGraphs<>())
      .withBroadcastSet(graphIds, ElementsOfSelectedGraphs.GRAPH_IDS)
      .groupBy(0)
      .combineGroup(new ApplyAggregateVertices(aggregateFunctions.stream()
        .filter(f -> f instanceof VertexAggregateFunction)
        .map(VertexAggregateFunction.class::cast)
        .collect(Collectors.toSet())));
  }

  /**
   * Applies an aggregate function to the partitions of an edge data set.
   *
   * @param edges edge data set
   * @param graphIds graph ids to aggregate
   * @return partition aggregate value
   */
  private DataSet<Tuple2<GradoopId, Map<String, PropertyValue>>> aggregateEdges(
    DataSet<Edge> edges, DataSet<GradoopId> graphIds) {
    return edges
      .flatMap(new ElementsOfSelectedGraphs<>())
      .withBroadcastSet(graphIds, ElementsOfSelectedGraphs.GRAPH_IDS)
      .groupBy(0)
      .combineGroup(new ApplyAggregateEdges(aggregateFunctions.stream()
        .filter(f -> f instanceof EdgeAggregateFunction)
        .map(EdgeAggregateFunction.class::cast)
        .collect(Collectors.toSet())));
  }

  @Override
  public String getName() {
    return ApplyAggregation.class.getName();
  }
}
