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
package org.gradoop.flink.model.impl.epgm;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.util.Order;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.gdl.GDLConsoleOutput;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.api.epgm.BaseGraphFactory;
import org.gradoop.flink.model.api.epgm.GraphCollectionOperators;
import org.gradoop.flink.model.api.functions.GraphHeadReduceFunction;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.operators.ApplicableUnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.BinaryCollectionToCollectionOperator;
import org.gradoop.flink.model.api.operators.ReducibleBinaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphCollectionToBaseGraphCollectionOperator;
import org.gradoop.flink.model.api.operators.UnaryCollectionToGraphOperator;
import org.gradoop.flink.model.impl.functions.bool.Not;
import org.gradoop.flink.model.impl.functions.bool.Or;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.functions.epgm.BySameId;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAnyGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.operators.difference.Difference;
import org.gradoop.flink.model.impl.operators.difference.DifferenceBroadcast;
import org.gradoop.flink.model.impl.operators.distinction.DistinctById;
import org.gradoop.flink.model.impl.operators.distinction.DistinctByIsomorphism;
import org.gradoop.flink.model.impl.operators.distinction.GroupByIsomorphism;
import org.gradoop.flink.model.impl.operators.equality.CollectionEquality;
import org.gradoop.flink.model.impl.operators.equality.CollectionEqualityByGraphIds;
import org.gradoop.flink.model.impl.operators.intersection.Intersection;
import org.gradoop.flink.model.impl.operators.intersection.IntersectionBroadcast;
import org.gradoop.flink.model.impl.operators.limit.Limit;
import org.gradoop.flink.model.impl.operators.matching.transactional.TransactionalPatternMatching;
import org.gradoop.flink.model.impl.operators.matching.transactional.algorithm.PatternMatchingAlgorithm;
import org.gradoop.flink.model.impl.operators.selection.Selection;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToIdString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToIdString;
import org.gradoop.flink.model.impl.operators.union.Union;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * A graph collection graph is one of the base concepts of the Extended Property Graph Model. From
 * a model perspective, the collection represents a set of logical graphs. From a data perspective
 * this is reflected by providing three concepts:
 *
 * - a set of graph heads assigned to the graphs in that collection
 * - a set of vertices which is the union of all vertex sets of the represented graphs
 * - a set of edges which is the union of all edge sets of the represented graphs
 *
 * Furthermore, a graph collection provides operations that are performed on the underlying data.
 * These operations result in either another graph collection or in a {@link LogicalGraph}.
 *
 * A graph collection is wrapping a {@link GraphCollectionLayout} which defines, how the collection
 * is represented in Apache Flink. Note that the GraphCollection also implements that interface and
 * just forward the calls to the layout. This is just for convenience and API synchronicity.
 */
public class GraphCollection implements
  BaseGraphCollection<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>,
  GraphCollectionOperators {
  /**
   * Layout for that graph collection
   */
  private final GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> layout;
  /**
   * Configuration
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a graph collection from the given arguments.
   *
   * @param layout the graph collection layout
   * @param config the Gradoop Flink configuration
   */
  GraphCollection(GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> layout,
    GradoopFlinkConfig config) {
    this.layout = Objects.requireNonNull(layout);
    this.config = Objects.requireNonNull(config);
  }

  //----------------------------------------------------------------------------
  // Data methods
  //----------------------------------------------------------------------------

  @Override
  public boolean isGVELayout() {
    return layout.isGVELayout();
  }

  @Override
  public boolean isIndexedGVELayout() {
    return layout.isIndexedGVELayout();
  }

  @Override
  public boolean isTransactionalLayout() {
    return layout.isTransactionalLayout();
  }

  @Override
  public DataSet<EPGMVertex> getVertices() {
    return layout.getVertices();
  }

  @Override
  public DataSet<EPGMVertex> getVerticesByLabel(String label) {
    return layout.getVerticesByLabel(label);
  }

  @Override
  public DataSet<EPGMEdge> getEdges() {
    return layout.getEdges();
  }

  @Override
  public DataSet<EPGMEdge> getEdgesByLabel(String label) {
    return layout.getEdgesByLabel(label);
  }

  @Override
  public DataSet<EPGMGraphHead> getGraphHeads() {
    return layout.getGraphHeads();
  }

  @Override
  public DataSet<EPGMGraphHead> getGraphHeadsByLabel(String label) {
    return layout.getGraphHeadsByLabel(label);
  }

  @Override
  public DataSet<GraphTransaction> getGraphTransactions() {
    return layout.getGraphTransactions();
  }

  //----------------------------------------------------------------------------
  // Logical Graph / Graph Head Getters
  //----------------------------------------------------------------------------

  @Override
  public LogicalGraph getGraph(final GradoopId graphID) {
    // filter vertices and edges based on given graph id
    DataSet<EPGMGraphHead> graphHead = getGraphHeads()
      .filter(new BySameId<>(graphID));
    DataSet<EPGMVertex> vertices = getVertices()
      .filter(new InGraph<>(graphID));
    DataSet<EPGMEdge> edges = getEdges()
      .filter(new InGraph<>(graphID));

    return new LogicalGraph(
      config.getLogicalGraphFactory().fromDataSets(graphHead, vertices, edges),
      getConfig());
  }

  @Override
  public GraphCollection getGraphs(final GradoopId... identifiers) {

    GradoopIdSet graphIds = new GradoopIdSet();

    graphIds.addAll(Arrays.asList(identifiers));

    return getGraphs(graphIds);
  }

  @Override
  public GraphCollection getGraphs(final GradoopIdSet identifiers) {

    DataSet<EPGMGraphHead> newGraphHeads = this.getGraphHeads()
      .filter(new FilterFunction<EPGMGraphHead>() {
        @Override
        public boolean filter(EPGMGraphHead graphHead) {
          return identifiers.contains(graphHead.getId());
        }
      });

    // build new vertex set
    DataSet<EPGMVertex> vertices = getVertices()
      .filter(new InAnyGraph<>(identifiers));

    // build new edge set
    DataSet<EPGMEdge> edges = getEdges()
      .filter(new InAnyGraph<>(identifiers));

    return new GraphCollection(getFactory().fromDataSets(newGraphHeads, vertices, edges),
      getConfig());
  }

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  @Override
  public GraphCollection select(final FilterFunction<EPGMGraphHead> predicate) {
    return callForCollection(new Selection(predicate));
  }

  @Override
  public GraphCollection sortBy(String propertyKey, Order order) {
    throw new NotImplementedException();
  }

  @Override
  public GraphCollection limit(int n) {
    return callForCollection(new Limit(n));
  }

  @Override
  public GraphCollection match(
    String pattern,
    PatternMatchingAlgorithm algorithm,
    boolean returnEmbeddings) {
    return new TransactionalPatternMatching(
      pattern,
      algorithm,
      returnEmbeddings).execute(this);
  }

  //----------------------------------------------------------------------------
  // Binary Operators
  //----------------------------------------------------------------------------

  @Override
  public GraphCollection union(GraphCollection otherCollection) {
    return callForCollection(new Union(), otherCollection);
  }

  @Override
  public GraphCollection intersect(GraphCollection otherCollection) {
    return callForCollection(new Intersection(), otherCollection);
  }

  @Override
  public GraphCollection intersectWithSmallResult(
    GraphCollection otherCollection) {
    return callForCollection(new IntersectionBroadcast(),
      otherCollection);
  }

  @Override
  public GraphCollection difference(GraphCollection otherCollection) {
    return callForCollection(new Difference(), otherCollection);
  }

  @Override
  public GraphCollection differenceWithSmallResult(
    GraphCollection otherCollection) {
    return callForCollection(new DifferenceBroadcast(),
      otherCollection);
  }

  @Override
  public DataSet<Boolean> equalsByGraphIds(GraphCollection other) {
    return new CollectionEqualityByGraphIds().execute(this, other);
  }

  @Override
  public DataSet<Boolean> equalsByGraphElementIds(GraphCollection other) {
    return new CollectionEquality(
      new GraphHeadToEmptyString(),
      new VertexToIdString(),
      new EdgeToIdString(), true).execute(this, other);
  }

  @Override
  public DataSet<Boolean> equalsByGraphElementData(GraphCollection other) {
    return new CollectionEquality(
      new GraphHeadToEmptyString(),
      new VertexToDataString(),
      new EdgeToDataString(), true).execute(this, other);
  }

  @Override
  public DataSet<Boolean> equalsByGraphData(GraphCollection other) {
    return new CollectionEquality(
      new GraphHeadToDataString(),
      new VertexToDataString(),
      new EdgeToDataString(), true).execute(this, other);
  }

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  @Override
  public GraphCollection callForCollection(
    UnaryBaseGraphCollectionToBaseGraphCollectionOperator<GraphCollection> operator) {
    return operator.execute(this);
  }

  @Override
  public GraphCollection callForCollection(
    BinaryCollectionToCollectionOperator op,
    GraphCollection otherCollection) {
    return op.execute(this, otherCollection);
  }

  @Override
  public LogicalGraph callForGraph(UnaryCollectionToGraphOperator op) {
    return op.execute(this);
  }

  @Override
  public GraphCollection apply(ApplicableUnaryGraphToGraphOperator op) {
    return callForCollection(op);
  }

  @Override
  public LogicalGraph reduce(ReducibleBinaryGraphToGraphOperator op) {
    return callForGraph(op);
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  @Override
  public GradoopFlinkConfig getConfig() {
    return config;
  }

  @Override
  public BaseGraphCollectionFactory<
    EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection> getFactory() {
    return config.getGraphCollectionFactory();
  }

  @Override
  public BaseGraphFactory<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>
  getGraphFactory() {
    return config.getLogicalGraphFactory();
  }

  @Override
  public DataSet<Boolean> isEmpty() {
    return getGraphHeads()
      .map(new True<>())
      .distinct()
      .union(getConfig().getExecutionEnvironment().fromElements(false))
      .reduce(new Or())
      .map(new Not());
  }

  @Override
  public GraphCollection distinctById() {
    return callForCollection(new DistinctById());
  }

  @Override
  public GraphCollection distinctByIsomorphism() {
    return callForCollection(new DistinctByIsomorphism());
  }

  @Override
  public GraphCollection groupByIsomorphism(GraphHeadReduceFunction func) {
    return callForCollection(new GroupByIsomorphism(func));
  }

  @Override
  public void writeTo(DataSink dataSink) throws IOException {
    dataSink.write(this);
  }

  @Override
  public void writeTo(DataSink dataSink, boolean overWrite) throws IOException {
    dataSink.write(this, overWrite);
  }

  /**
   * Prints this graph collection to the console.
   *
   * @throws Exception forwarded DataSet print() Exception.
   */
  public void print() throws Exception {
    GDLConsoleOutput.print(this);
  }
}
