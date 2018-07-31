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
package org.gradoop.flink.model.impl.layouts.gve;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
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
public class GVECollectionLayoutFactory extends GVEBaseFactory implements GraphCollectionLayoutFactory {

  @Override
  public GraphCollectionLayout fromDataSets(DataSet<GraphHead> graphHeads,
    DataSet<Vertex> vertices) {
    return fromDataSets(graphHeads, vertices,
      createEdgeDataSet(new ArrayList<>(0)));
  }

  @Override
  public GraphCollectionLayout fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices,
    DataSet<Edge> edges) {
    return create(graphHeads, vertices, edges);
  }

  @Override
  public GraphCollectionLayout fromIndexedDataSets(Map<String, DataSet<GraphHead>> graphHeads,
    Map<String, DataSet<Vertex>> vertices, Map<String, DataSet<Edge>> edges) {
    return create(graphHeads, vertices, edges);
  }

  @Override
  public GraphCollectionLayout fromCollections(Collection<GraphHead> graphHeads,
    Collection<Vertex> vertices, Collection<Edge> edges) {
    Objects.requireNonNull(graphHeads, "GraphHead collection was null");
    Objects.requireNonNull(vertices, "Vertex collection was null");
    Objects.requireNonNull(edges, "Edge collection was null");
    return fromDataSets(
      createGraphHeadDataSet(graphHeads),
      createVertexDataSet(vertices),
      createEdgeDataSet(edges));
  }

  @Override
  public GraphCollectionLayout fromGraphLayout(LogicalGraphLayout graph) {
    return fromDataSets(graph.getGraphHead(), graph.getVertices(), graph.getEdges());
  }

  @Override
  public GraphCollectionLayout fromTransactions(DataSet<GraphTransaction> transactions) {
    GroupReduceFunction<Vertex, Vertex> vertexReducer = new First<>();
    GroupReduceFunction<Edge, Edge> edgeReducer = new First<>();

    return fromTransactions(transactions, vertexReducer, edgeReducer);
  }

  @Override
  public GraphCollectionLayout fromTransactions(DataSet<GraphTransaction> transactions,
    GroupReduceFunction<Vertex, Vertex> vertexMergeReducer,
    GroupReduceFunction<Edge, Edge> edgeMergeReducer) {

    DataSet<GraphHead> graphHeads = transactions.map(new TransactionGraphHead<>());

    DataSet<Vertex> vertices = transactions
      .flatMap(new TransactionVertices<>())
      .groupBy(new Id<>())
      .reduceGroup(vertexMergeReducer);

    DataSet<Edge> edges = transactions
      .flatMap(new TransactionEdges<>())
      .groupBy(new Id<>())
      .reduceGroup(edgeMergeReducer);

    return fromDataSets(graphHeads, vertices, edges);
  }

  @Override
  public GraphCollectionLayout createEmptyCollection() {
    Collection<GraphHead> graphHeads = new ArrayList<>();
    Collection<Vertex> vertices = new ArrayList<>();
    Collection<Edge> edges = new ArrayList<>();

    return fromCollections(graphHeads, vertices, edges);
  }
}
