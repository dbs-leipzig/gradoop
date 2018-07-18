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
package org.gradoop.flink.model.impl.layouts.transactional;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.impl.functions.bool.False;
import org.gradoop.flink.model.impl.functions.epgm.GraphElementExpander;
import org.gradoop.flink.model.impl.functions.epgm.GraphVerticesEdges;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.TransactionFromSets;
import org.gradoop.flink.model.impl.functions.utils.Cast;
import org.gradoop.flink.model.impl.layouts.common.BaseFactory;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Responsible for producing instances of {@link TxCollectionLayout}.
 */
public class TxCollectionLayoutFactory extends BaseFactory implements GraphCollectionLayoutFactory {

  @Override
  public GraphCollectionLayout fromDataSets(DataSet<GraphHead> graphHeads,
    DataSet<Vertex> vertices) {
    Objects.requireNonNull(graphHeads);
    Objects.requireNonNull(vertices);

    return fromDataSets(graphHeads, vertices,
      createEdgeDataSet(Lists.newArrayListWithCapacity(0)));
  }

  @Override
  public GraphCollectionLayout fromDataSets(DataSet<GraphHead> inGraphHeads,
    DataSet<Vertex> inVertices, DataSet<Edge> inEdges) {
    Objects.requireNonNull(inGraphHeads);
    Objects.requireNonNull(inVertices);
    Objects.requireNonNull(inEdges);

    // Add a dummy graph head for entities which have no assigned graph
    DataSet<GraphHead> dbGraphHead = getConfig().getExecutionEnvironment().fromElements(
      getConfig().getGraphHeadFactory()
        .initGraphHead(GradoopConstants.DB_GRAPH_ID, GradoopConstants.DB_GRAPH_LABEL)
    );
    inGraphHeads = inGraphHeads.union(dbGraphHead);

    DataSet<Tuple2<GradoopId, GraphElement>> vertices = inVertices
      .map(new Cast<>(GraphElement.class))
      .returns(TypeExtractor.getForClass(GraphElement.class))
      .flatMap(new GraphElementExpander<>());

    DataSet<Tuple2<GradoopId, GraphElement>> edges = inEdges
      .map(new Cast<>(GraphElement.class))
      .returns(TypeExtractor.getForClass(GraphElement.class))
      .flatMap(new GraphElementExpander<>());

    DataSet<Tuple3<GradoopId, Set<Vertex>, Set<Edge>>> transactions = vertices
      .union(edges)
      .groupBy(0)
      .combineGroup(new GraphVerticesEdges())
      .groupBy(0)
      .reduceGroup(new GraphVerticesEdges());

    DataSet<GraphTransaction> graphTransactions = inGraphHeads
      .leftOuterJoin(transactions)
      .where(new Id<>()).equalTo(0)
      .with(new TransactionFromSets());

    return new TxCollectionLayout(graphTransactions);
  }

  @Override
  public GraphCollectionLayout fromIndexedDataSets(Map<String, DataSet<GraphHead>> graphHeads,
    Map<String, DataSet<Vertex>> vertices, Map<String, DataSet<Edge>> edges) {
    Objects.requireNonNull(graphHeads);
    Objects.requireNonNull(vertices);
    Objects.requireNonNull(edges);
    return fromDataSets(
      graphHeads.values().stream().reduce(DataSet::union)
        .orElseThrow(() -> new RuntimeException("Error during graph head union")),
      vertices.values().stream().reduce(DataSet::union)
        .orElseThrow(() -> new RuntimeException("Error during vertex union")),
      edges.values().stream().reduce(DataSet::union)
        .orElseThrow(() -> new RuntimeException("Error during edge union"))
    );
  }

  @Override
  public GraphCollectionLayout fromCollections(Collection<GraphHead> graphHeads,
    Collection<Vertex> vertices, Collection<Edge> edges) {
    return fromDataSets(
      createGraphHeadDataSet(graphHeads),
      createVertexDataSet(vertices),
      createEdgeDataSet(edges)
    );
  }

  @Override
  public GraphCollectionLayout fromGraphLayout(LogicalGraphLayout logicalGraphLayout) {
    return fromDataSets(logicalGraphLayout.getGraphHead(),
      logicalGraphLayout.getVertices(),
      logicalGraphLayout.getEdges());
  }

  @Override
  public GraphCollectionLayout fromTransactions(DataSet<GraphTransaction> transactions) {
    return new TxCollectionLayout(transactions);
  }

  @Override
  public GraphCollectionLayout fromTransactions(DataSet<GraphTransaction> transactions,
    GroupReduceFunction<Vertex, Vertex> vertexMergeReducer,
    GroupReduceFunction<Edge, Edge> edgeMergeReducer) {
    return new TxCollectionLayout(transactions);
  }

  @Override
  public GraphCollectionLayout createEmptyCollection() {
    return fromTransactions(createGraphTransactionDataSet(Lists.newArrayListWithCapacity(0)));
  }

  /**
   * Creates a dataset from a given (possibly empty) collection of graph transactions.
   *
   * @param transactions graph transactions
   * @return a dataset containing the given transactions
   */
  private DataSet<GraphTransaction> createGraphTransactionDataSet(
    Collection<GraphTransaction> transactions) {
    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    DataSet<GraphTransaction> graphTransactionSet;
    if (transactions.isEmpty()) {
      graphTransactionSet = env.fromCollection(Lists.newArrayList(new GraphTransaction()),
        new TypeHint<GraphTransaction>() { }.getTypeInfo())
        .filter(new False<>());
    } else {
      graphTransactionSet = env.fromCollection(transactions);
    }
    return graphTransactionSet;
  }
}
