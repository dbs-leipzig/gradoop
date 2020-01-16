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
package org.gradoop.flink.model.impl.layouts.gve;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.TransactionEdges;
import org.gradoop.flink.model.impl.functions.epgm.TransactionGraphHead;
import org.gradoop.flink.model.impl.functions.epgm.TransactionVertices;
import org.gradoop.flink.model.impl.functions.utils.First;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Responsible for creating a {@link GVELayout} from given data.
 */
public class GVECollectionLayoutFactory extends GVEBaseFactory
  implements GraphCollectionLayoutFactory<EPGMGraphHead, EPGMVertex, EPGMEdge> {

  @Override
  public GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromDataSets(
    DataSet<EPGMGraphHead> graphHeads, DataSet<EPGMVertex> vertices) {
    return fromDataSets(graphHeads, vertices,
      createEdgeDataSet(new ArrayList<>(0)));
  }

  @Override
  public GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromDataSets(
    DataSet<EPGMGraphHead> graphHeads,
    DataSet<EPGMVertex> vertices,
    DataSet<EPGMEdge> edges) {
    return create(graphHeads, vertices, edges);
  }

  @Override
  public GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromIndexedDataSets(
    Map<String, DataSet<EPGMGraphHead>> graphHeads,
    Map<String, DataSet<EPGMVertex>> vertices,
    Map<String, DataSet<EPGMEdge>> edges) {
    return create(graphHeads, vertices, edges);
  }

  @Override
  public GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromCollections(
    Collection<EPGMGraphHead> graphHeads,
    Collection<EPGMVertex> vertices,
    Collection<EPGMEdge> edges) {
    return fromDataSets(
      createGraphHeadDataSet(Objects.requireNonNull(graphHeads, "EPGMGraphHead collection was null")),
      createVertexDataSet(Objects.requireNonNull(vertices, "EPGMVertex collection was null")),
      createEdgeDataSet(Objects.requireNonNull(edges, "EPGMEdge collection was null")));
  }

  @Override
  public GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromGraphLayout(
    LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> graph) {
    return fromDataSets(graph.getGraphHead(), graph.getVertices(), graph.getEdges());
  }

  @Override
  public GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromTransactions(
    DataSet<GraphTransaction> transactions) {
    GroupReduceFunction<EPGMVertex, EPGMVertex> vertexReducer = new First<>();
    GroupReduceFunction<EPGMEdge, EPGMEdge> edgeReducer = new First<>();

    return fromTransactions(transactions, vertexReducer, edgeReducer);
  }

  @Override
  public GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromTransactions(
    DataSet<GraphTransaction> transactions,
    GroupReduceFunction<EPGMVertex, EPGMVertex> vertexMergeReducer,
    GroupReduceFunction<EPGMEdge, EPGMEdge> edgeMergeReducer) {

    DataSet<EPGMGraphHead> graphHeads = transactions.map(new TransactionGraphHead<>());

    DataSet<EPGMVertex> vertices = transactions
      .flatMap(new TransactionVertices<>())
      .groupBy(new Id<>())
      .reduceGroup(vertexMergeReducer);

    DataSet<EPGMEdge> edges = transactions
      .flatMap(new TransactionEdges<>())
      .groupBy(new Id<>())
      .reduceGroup(edgeMergeReducer);

    return fromDataSets(graphHeads, vertices, edges);
  }

  @Override
  public GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> createEmptyCollection() {
    Collection<EPGMGraphHead> graphHeads = new ArrayList<>();
    Collection<EPGMVertex> vertices = new ArrayList<>();
    Collection<EPGMEdge> edges = new ArrayList<>();

    return fromCollections(graphHeads, vertices, edges);
  }
}
