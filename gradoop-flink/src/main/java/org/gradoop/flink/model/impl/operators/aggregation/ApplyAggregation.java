/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.ApplicableUnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.functions.epgm.ElementsOfSelectedGraphs;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.AggregateTransactions;
import org.gradoop.flink.model.impl.operators.aggregation.functions.ApplyAggregateElements;
import org.gradoop.flink.model.impl.operators.aggregation.functions.CombinePartitionApplyAggregates;
import org.gradoop.flink.model.impl.operators.aggregation.functions.SetAggregateProperties;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Takes a collection of base graphs and user defined aggregate functions as
 * input. The aggregate functions are applied on each base graph contained in
 * the collection and the aggregate is stored as additional properties at the graphs.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> type of the logical graph instance
 * @param <GC> type of the graph collection
 */
public class ApplyAggregation<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  implements ApplicableUnaryBaseGraphToBaseGraphOperator<GC> {

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

  @Override
  public GC executeForGVELayout(GC collection) {
    DataSet<G> graphHeads = collection.getGraphHeads();
    DataSet<GradoopId> graphIds = graphHeads.map(new Id<>());

    DataSet<Tuple2<GradoopId, Map<String, PropertyValue>>> aggregate =
      aggregateVertices(collection.getVertices(), graphIds)
        .union(aggregateEdges(collection.getEdges(), graphIds))
        .groupBy(0)
        .reduceGroup(new CombinePartitionApplyAggregates(aggregateFunctions));

    graphHeads = graphHeads
      .coGroup(aggregate)
      .where(new Id<>()).equalTo(0)
      .with(new SetAggregateProperties<>(aggregateFunctions));

    return collection.getFactory().fromDataSets(graphHeads, collection.getVertices(), collection.getEdges());
  }

  @Override
  public GC executeForTxLayout(GC collection) {
    DataSet<GraphTransaction> updatedTransactions = collection.getGraphTransactions()
      .map(new AggregateTransactions(aggregateFunctions));

    return collection.getFactory().fromTransactions(updatedTransactions);
  }

  /**
   * Applies an aggregate function to the partitions of a vertex data set.
   *
   * @param vertices vertex data set
   * @param graphIds graph ids to aggregate
   * @return partition aggregate value
   */
  private DataSet<Tuple2<GradoopId, Map<String, PropertyValue>>> aggregateVertices(
    DataSet<V> vertices, DataSet<GradoopId> graphIds) {
    return vertices
      .flatMap(new ElementsOfSelectedGraphs<>())
      .withBroadcastSet(graphIds, ElementsOfSelectedGraphs.GRAPH_IDS)
      .groupBy(0)
      .combineGroup(new ApplyAggregateElements<>(aggregateFunctions.stream()
        .filter(AggregateFunction::isVertexAggregation)
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
    DataSet<E> edges, DataSet<GradoopId> graphIds) {
    return edges
      .flatMap(new ElementsOfSelectedGraphs<>())
      .withBroadcastSet(graphIds, ElementsOfSelectedGraphs.GRAPH_IDS)
      .groupBy(0)
      .combineGroup(new ApplyAggregateElements<>(aggregateFunctions.stream()
        .filter(AggregateFunction::isEdgeAggregation)
        .collect(Collectors.toSet())));
  }
}
