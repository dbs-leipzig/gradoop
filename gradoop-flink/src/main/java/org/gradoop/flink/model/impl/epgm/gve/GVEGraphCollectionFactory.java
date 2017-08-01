/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.epgm.gve;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.GraphCollectionFactory;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.epgm.transactional.GraphTransactions;
import org.gradoop.flink.model.impl.functions.epgm.GraphTransactionTriple;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.TransactionEdges;
import org.gradoop.flink.model.impl.functions.epgm.TransactionGraphHead;
import org.gradoop.flink.model.impl.functions.epgm.TransactionVertices;
import org.gradoop.flink.model.impl.functions.utils.First;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * Responsible for producing GVE-based graph collections.
 */
public class GVEGraphCollectionFactory extends GVEGraphBaseFactory implements GraphCollectionFactory {

  /**
   * Creates a new graph collection factory.
   *
   * @param config Gradoop config
   */
  public GVEGraphCollectionFactory(GradoopFlinkConfig config) {
    super(config);
  }

  @Override
  public GraphCollection fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices) {
    return fromDataSets(graphHeads, vertices, createEdgeDataSet(new ArrayList<>(0)));
  }

  @Override
  public GraphCollection fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices,
    DataSet<Edge> edges) {
    Objects.requireNonNull(graphHeads, "GraphHead DataSet was null");
    Objects.requireNonNull(vertices, "Vertex DataSet was null");
    Objects.requireNonNull(edges, "Edge DataSet was null");
    Objects.requireNonNull(config, "Config was null");
    return new GVEGraphCollection(graphHeads, vertices, edges, config);
  }

  @Override
  public GraphCollection fromCollections(Collection<GraphHead> graphHeads,
    Collection<Vertex> vertices, Collection<Edge> edges) {
    Objects.requireNonNull(graphHeads, "GraphHead collection was null");
    Objects.requireNonNull(vertices, "Vertex collection was null");
    Objects.requireNonNull(edges, "Vertex collection was null");
    return fromDataSets(
      createGraphHeadDataSet(graphHeads),
      createVertexDataSet(vertices),
      createEdgeDataSet(edges));
  }

  @Override
  public GraphCollection fromGraph(LogicalGraph graph) {
    return fromDataSets(graph.getGraphHead(), graph.getVertices(), graph.getEdges());
  }

  @Override
  public GraphCollection fromTransactions(GraphTransactions transactions) {
    GroupReduceFunction<Vertex, Vertex> vertexReducer = new First<>();
    GroupReduceFunction<Edge, Edge> edgeReducer = new First<>();

    return fromTransactions(transactions, vertexReducer, edgeReducer);
  }

  @Override
  public GraphCollection fromTransactions(GraphTransactions transactions,
    GroupReduceFunction<Vertex, Vertex> vertexMergeReducer,
    GroupReduceFunction<Edge, Edge> edgeMergeReducer) {

    DataSet<Tuple3<GraphHead, Set<Vertex>, Set<Edge>>> triples = transactions
      .getTransactions()
      .map(new GraphTransactionTriple());

    DataSet<GraphHead> graphHeads = triples.map(new TransactionGraphHead());

    DataSet<Vertex> vertices = triples
      .flatMap(new TransactionVertices())
      .groupBy(new Id<>())
      .reduceGroup(vertexMergeReducer);

    DataSet<Edge> edges = triples
      .flatMap(new TransactionEdges())
      .groupBy(new Id<>())
      .reduceGroup(edgeMergeReducer);

    return fromDataSets(graphHeads, vertices, edges);
  }

  @Override
  public GraphCollection createEmptyCollection() {
    Collection<GraphHead> graphHeads = new ArrayList<>();
    Collection<Vertex> vertices = new ArrayList<>();
    Collection<Edge> edges = new ArrayList<>();

    return fromCollections(graphHeads, vertices, edges);
  }
}
