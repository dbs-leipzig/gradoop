/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMGraphElement;
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
public class TxCollectionLayoutFactory extends BaseFactory
  implements GraphCollectionLayoutFactory<EPGMGraphHead, EPGMVertex, EPGMEdge> {

  @Override
  public GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromDataSets(
    DataSet<EPGMGraphHead> graphHeads, DataSet<EPGMVertex> vertices) {

    Objects.requireNonNull(graphHeads);
    Objects.requireNonNull(vertices);

    return fromDataSets(graphHeads, vertices,
      createEdgeDataSet(Lists.newArrayListWithCapacity(0)));
  }

  @Override
  public GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromDataSets(
    DataSet<EPGMGraphHead> inGraphHeads,
    DataSet<EPGMVertex> inVertices,
    DataSet<EPGMEdge> inEdges) {
    Objects.requireNonNull(inGraphHeads);
    Objects.requireNonNull(inVertices);
    Objects.requireNonNull(inEdges);

    // Add a dummy graph head for entities which have no assigned graph
    DataSet<EPGMGraphHead> dbGraphHead = getConfig().getExecutionEnvironment().fromElements(
      getGraphHeadFactory()
        .initGraphHead(GradoopConstants.DB_GRAPH_ID, GradoopConstants.DB_GRAPH_LABEL)
    );
    inGraphHeads = inGraphHeads.union(dbGraphHead);

    DataSet<Tuple2<GradoopId, EPGMGraphElement>> vertices = inVertices
      .map(new Cast<>(EPGMGraphElement.class))
      .returns(TypeExtractor.getForClass(EPGMGraphElement.class))
      .flatMap(new GraphElementExpander<>());

    DataSet<Tuple2<GradoopId, EPGMGraphElement>> edges = inEdges
      .map(new Cast<>(EPGMGraphElement.class))
      .returns(TypeExtractor.getForClass(EPGMGraphElement.class))
      .flatMap(new GraphElementExpander<>());

    DataSet<Tuple3<GradoopId, Set<EPGMVertex>, Set<EPGMEdge>>> transactions = vertices
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
  public GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromIndexedDataSets(
    Map<String, DataSet<EPGMGraphHead>> graphHeads,
    Map<String, DataSet<EPGMVertex>> vertices,
    Map<String, DataSet<EPGMEdge>> edges) {
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
  public GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromCollections(
    Collection<EPGMGraphHead> graphHeads,
    Collection<EPGMVertex> vertices,
    Collection<EPGMEdge> edges) {
    return fromDataSets(
      createGraphHeadDataSet(graphHeads),
      createVertexDataSet(vertices),
      createEdgeDataSet(edges)
    );
  }

  @Override
  public GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromGraphLayout(
    LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> logicalGraphLayout) {
    return fromDataSets(logicalGraphLayout.getGraphHead(),
      logicalGraphLayout.getVertices(),
      logicalGraphLayout.getEdges());
  }

  @Override
  public GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromTransactions(
    DataSet<GraphTransaction> transactions) {
    return new TxCollectionLayout(transactions);
  }

  @Override
  public GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromTransactions(
    DataSet<GraphTransaction> transactions,
    GroupReduceFunction<EPGMVertex, EPGMVertex> vertexMergeReducer,
    GroupReduceFunction<EPGMEdge, EPGMEdge> edgeMergeReducer) {
    return new TxCollectionLayout(transactions);
  }

  @Override
  public GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> createEmptyCollection() {
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
