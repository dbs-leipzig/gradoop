/**
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

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Takes a collection of logical graphs and a user defined aggregate function as
 * input. The aggregate function is applied on each logical graph contained in
 * the collection and the aggregate is stored as an additional property at the
 * graphs.
 */
public class ApplyAggregation
  implements ApplicableUnaryGraphToGraphOperator {

  /**
   * User-defined aggregate function which is applied on a graph collection.
   */
  private final AggregateFunction aggregateFunction;

  /**
   * Creates a new operator instance.
   *
   * @param aggregateFunction     function to compute aggregate value
   */
  public ApplyAggregation(final AggregateFunction aggregateFunction) {
    this.aggregateFunction = checkNotNull(aggregateFunction);
  }

  @Override
  public GraphCollection execute(GraphCollection collection) {
    return (collection.isTransactionalLayout()) ?
      executeForTxLayout(collection) :
      executeForGVELayout(collection);
  }

  /**
   * Executes the operator for collections based on
   * {@link org.gradoop.flink.model.impl.layouts.gve.GVELayout}
   *
   * @param collection graph collection
   * @return result graph collection
   */
  private GraphCollection executeForGVELayout(GraphCollection collection) {
    DataSet<GraphHead> graphHeads = collection.getGraphHeads();
    DataSet<Vertex> vertices = collection.getVertices();
    DataSet<Edge> edges = collection.getEdges();

    DataSet<Tuple2<GradoopId, PropertyValue>> aggregate;

    DataSet<GradoopId> graphIds = graphHeads
      .map(new Id<>());

    if (this.aggregateFunction instanceof VertexAggregateFunction) {
      aggregate = aggregateVertices(vertices, graphIds);
    } else {
      aggregate = aggregateEdges(edges, graphIds);
    }

    aggregate = aggregate
      .groupBy(0)
      .reduceGroup(new CombinePartitionApplyAggregates(aggregateFunction));

    graphHeads = graphHeads
      .coGroup(aggregate)
      .where(new Id<>()).equalTo(0)
      .with(new SetAggregateProperties(aggregateFunction));

    return collection.getConfig().getGraphCollectionFactory()
      .fromDataSets(graphHeads, collection.getVertices(), collection.getEdges());
  }

  /**
   * Executes the operator for collections based on
   * {@link org.gradoop.flink.model.impl.layouts.transactional.TxCollectionLayout}
   *
   * @param collection graph collection
   * @return result graph collection
   */
  private GraphCollection executeForTxLayout(GraphCollection collection) {
    DataSet<GraphTransaction> updatedTransactions = collection.getGraphTransactions()
      .map(new AggregateTransactions(this.aggregateFunction));

    return collection.getConfig().getGraphCollectionFactory().fromTransactions(updatedTransactions);
  }

  /**
   * Applies an aggregate function to the partitions of a vertex data set.
   *
   * @param vertices vertex data set
   * @param graphIds graph ids to aggregate
   * @return partition aggregate value
   */
  private DataSet<Tuple2<GradoopId, PropertyValue>> aggregateVertices(
    DataSet<Vertex> vertices, DataSet<GradoopId> graphIds) {
    return vertices
      .flatMap(new ElementsOfSelectedGraphs<>())
      .withBroadcastSet(graphIds, ElementsOfSelectedGraphs.GRAPH_IDS)
      .groupBy(0)
      .combineGroup(new ApplyAggregateVertices(
        (VertexAggregateFunction) aggregateFunction));
  }

  /**
   * Applies an aggregate function to the partitions of an edge data set.
   *
   * @param edges edge data set
   * @param graphIds graph ids to aggregate
   * @return partition aggregate value
   */
  private DataSet<Tuple2<GradoopId, PropertyValue>> aggregateEdges(
    DataSet<Edge> edges, DataSet<GradoopId> graphIds) {
    return edges
      .flatMap(new ElementsOfSelectedGraphs<>())
      .withBroadcastSet(graphIds, ElementsOfSelectedGraphs.GRAPH_IDS)
      .groupBy(0)
      .combineGroup(new ApplyAggregateEdges(
        (EdgeAggregateFunction) aggregateFunction));
  }

  @Override
  public String getName() {
    return ApplyAggregation.class.getName();
  }
}
