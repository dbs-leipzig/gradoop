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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.metadata.MetaData;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.gdl.GDLConsoleOutput;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.api.epgm.BaseGraphFactory;
import org.gradoop.flink.model.api.epgm.LogicalGraphOperators;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.GraphsToGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.flink.model.impl.functions.bool.Not;
import org.gradoop.flink.model.impl.functions.bool.Or;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.functions.epgm.PropertyGetter;
import org.gradoop.flink.model.impl.operators.aggregation.Aggregation;
import org.gradoop.flink.model.impl.operators.cloning.Cloning;
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.model.impl.operators.cypher.capf.query.CAPFQuery;
import org.gradoop.flink.model.impl.operators.cypher.capf.result.CAPFQueryResult;
import org.gradoop.flink.model.impl.operators.equality.GraphEquality;
import org.gradoop.flink.model.impl.operators.exclusion.Exclusion;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.CypherPatternMatching;
import org.gradoop.flink.model.impl.operators.neighborhood.Neighborhood;
import org.gradoop.flink.model.impl.operators.neighborhood.ReduceEdgeNeighborhood;
import org.gradoop.flink.model.impl.operators.neighborhood.ReduceVertexNeighborhood;
import org.gradoop.flink.model.impl.operators.overlap.Overlap;
import org.gradoop.flink.model.impl.operators.rollup.EdgeRollUp;
import org.gradoop.flink.model.impl.operators.rollup.VertexRollUp;
import org.gradoop.flink.model.impl.operators.sampling.SamplingAlgorithm;
import org.gradoop.flink.model.impl.operators.split.Split;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToIdString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToIdString;
import org.gradoop.flink.model.impl.operators.transformation.Transformation;
import org.gradoop.flink.model.impl.operators.verify.Verify;
import org.gradoop.flink.model.impl.operators.verify.VerifyGraphContainment;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A logical graph is one of the base concepts of the Extended Property Graph Model. A logical graph
 * encapsulates three concepts:
 * <p>
 * - a so-called graph head, that stores information about the graph (i.e. label and properties)
 * - a set of vertices assigned to the graph
 * - a set of directed, possibly parallel edges assigned to the graph
 * <p>
 * Furthermore, a logical graph provides operations that are performed on the underlying data. These
 * operations result in either another logical graph or in a {@link GraphCollection}.
 * <p>
 * A logical graph is wrapping a {@link LogicalGraphLayout} which defines, how the graph is
 * represented in Apache Flink. Note that the LogicalGraph also implements that interface and
 * just forward the calls to the layout. This is just for convenience and API synchronicity.
 */
public class LogicalGraph implements
  BaseGraph<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>, LogicalGraphOperators {
  /**
   * Layout for that logical graph.
   */
  private final LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> layout;
  /**
   * Configuration
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a new logical graph based on the given parameters.
   *
   * @param layout representation of the logical graph
   * @param config the Gradoop Flink configuration
   */
  LogicalGraph(LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> layout,
    GradoopFlinkConfig config) {
    Objects.requireNonNull(layout);
    Objects.requireNonNull(config);
    this.layout = layout;
    this.config = config;
  }

  //----------------------------------------------------------------------------
  // Data methods
  //----------------------------------------------------------------------------

  @Override
  public GradoopFlinkConfig getConfig() {
    return config;
  }

  @Override
  public BaseGraphFactory<
    EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection> getFactory() {
    return config.getLogicalGraphFactory();
  }

  @Override
  public BaseGraphCollectionFactory<
    EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection> getCollectionFactory() {
    return config.getGraphCollectionFactory();
  }

  @Override
  public boolean isGVELayout() {
    return layout.isGVELayout();
  }

  @Override
  public boolean isIndexedGVELayout() {
    return layout.isIndexedGVELayout();
  }

  @Override
  public DataSet<EPGMGraphHead> getGraphHead() {
    return layout.getGraphHead();
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

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  @Override
  public CAPFQueryResult cypher(String query) throws Exception {
    CAPFQuery capfQuery = new CAPFQuery(
      query, this.config.getExecutionEnvironment()
    );
    return capfQuery.execute(this);
  }

  @Override
  public CAPFQueryResult cypher(String query, MetaData metaData) throws Exception {
    CAPFQuery capfQuery = new CAPFQuery(
      query, metaData, this.config.getExecutionEnvironment());
    return capfQuery.execute(this);
  }

  @Override
  public GraphCollection query(String query) {
    return query(query, new GraphStatistics(1, 1, 1, 1));
  }

  @Override
  public GraphCollection query(String query, String constructionPattern) {
    return query(query, constructionPattern, new GraphStatistics(1, 1, 1, 1));
  }

  @Override
  public GraphCollection query(String query, GraphStatistics graphStatistics) {
    return query(query, true,
      MatchStrategy.HOMOMORPHISM, MatchStrategy.ISOMORPHISM, graphStatistics);
  }

  @Override
  public GraphCollection query(String query, String constructionPattern,
    GraphStatistics graphStatistics) {
    return query(query, constructionPattern, true,
      MatchStrategy.HOMOMORPHISM, MatchStrategy.ISOMORPHISM, graphStatistics);
  }

  @Override
  public GraphCollection query(String query, boolean attachData, MatchStrategy vertexStrategy,
    MatchStrategy edgeStrategy, GraphStatistics graphStatistics) {
    return query(query, null, attachData, vertexStrategy, edgeStrategy, graphStatistics);
  }

  @Override
  public GraphCollection query(String query, String constructionPattern, boolean attachData,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy,
    GraphStatistics graphStatistics) {
    return callForCollection(new CypherPatternMatching(query, constructionPattern, attachData,
      vertexStrategy, edgeStrategy, graphStatistics));
  }

  @Override
  public LogicalGraph copy() {
    return callForGraph(new Cloning());
  }

  @Override
  public LogicalGraph transform(
    TransformationFunction<EPGMGraphHead> graphHeadTransformationFunction,
    TransformationFunction<EPGMVertex> vertexTransformationFunction,
    TransformationFunction<EPGMEdge> edgeTransformationFunction) {
    return callForGraph(new Transformation<>(
      graphHeadTransformationFunction,
      vertexTransformationFunction,
      edgeTransformationFunction));
  }

  @Override
  public LogicalGraph transformGraphHead(
    TransformationFunction<EPGMGraphHead> graphHeadTransformationFunction) {
    return transform(graphHeadTransformationFunction, null, null);
  }

  @Override
  public LogicalGraph transformVertices(
    TransformationFunction<EPGMVertex> vertexTransformationFunction) {
    return transform(null, vertexTransformationFunction, null);
  }

  @Override
  public LogicalGraph transformEdges(
    TransformationFunction<EPGMEdge> edgeTransformationFunction) {
    return transform(null, null, edgeTransformationFunction);
  }

  @Override
  public LogicalGraph vertexInducedSubgraph(
    FilterFunction<EPGMVertex> vertexFilterFunction) {
    Objects.requireNonNull(vertexFilterFunction);
    return callForGraph(
      new Subgraph<>(vertexFilterFunction, null, Subgraph.Strategy.VERTEX_INDUCED));
  }

  @Override
  public LogicalGraph edgeInducedSubgraph(
    FilterFunction<EPGMEdge> edgeFilterFunction) {
    Objects.requireNonNull(edgeFilterFunction);
    return callForGraph(new Subgraph<>(null, edgeFilterFunction, Subgraph.Strategy.EDGE_INDUCED));
  }

  @Override
  public LogicalGraph subgraph(
    FilterFunction<EPGMVertex> vertexFilterFunction,
    FilterFunction<EPGMEdge> edgeFilterFunction, Subgraph.Strategy strategy) {

    return callForGraph(
      new Subgraph<>(vertexFilterFunction, edgeFilterFunction, strategy));
  }

  @Override
  public LogicalGraph aggregate(AggregateFunction... aggregateFunctions) {
    return callForGraph(new Aggregation<>(aggregateFunctions));
  }

  @Override
  public LogicalGraph sample(SamplingAlgorithm algorithm) {
    return callForGraph(algorithm);
  }

  @Override
  public LogicalGraph groupBy(List<String> vertexGroupingKeys) {
    return groupBy(vertexGroupingKeys, null);
  }

  @Override
  public LogicalGraph groupBy(List<String> vertexGroupingKeys, List<String> edgeGroupingKeys) {
    return groupBy(vertexGroupingKeys, null, edgeGroupingKeys, null, GroupingStrategy.GROUP_REDUCE);
  }

  @Override
  public LogicalGraph groupBy(
    List<String> vertexGroupingKeys, List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<AggregateFunction> edgeAggregateFunctions,
    GroupingStrategy groupingStrategy) {

    Objects.requireNonNull(vertexGroupingKeys, "missing vertex grouping key(s)");
    Objects.requireNonNull(groupingStrategy, "missing vertex grouping strategy");

    Grouping.GroupingBuilder builder = new Grouping.GroupingBuilder();

    builder.addVertexGroupingKeys(vertexGroupingKeys);
    builder.setStrategy(groupingStrategy);

    if (edgeGroupingKeys != null) {
      builder.addEdgeGroupingKeys(edgeGroupingKeys);
    }
    if (vertexAggregateFunctions != null) {
      vertexAggregateFunctions.forEach(builder::addVertexAggregateFunction);
    }
    if (edgeAggregateFunctions != null) {
      edgeAggregateFunctions.forEach(builder::addEdgeAggregateFunction);
    }
    return callForGraph(builder.build());
  }

  @Override
  public LogicalGraph reduceOnEdges(
    EdgeAggregateFunction function, Neighborhood.EdgeDirection edgeDirection) {
    return callForGraph(new ReduceEdgeNeighborhood(function, edgeDirection));
  }

  @Override
  public LogicalGraph reduceOnNeighbors(
    VertexAggregateFunction function, Neighborhood.EdgeDirection edgeDirection) {
    return callForGraph(new ReduceVertexNeighborhood(function, edgeDirection));
  }

  @Override
  public GraphCollection groupVerticesByRollUp(
    List<String> vertexGroupingKeys, List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<AggregateFunction> edgeAggregateFunctions) {
    if (vertexGroupingKeys == null || vertexGroupingKeys.isEmpty()) {
      throw new IllegalArgumentException("Missing vertex grouping key(s).");
    }

    return callForCollection(new VertexRollUp(vertexGroupingKeys, vertexAggregateFunctions,
      edgeGroupingKeys, edgeAggregateFunctions));
  }

  @Override
  public GraphCollection groupEdgesByRollUp(
    List<String> vertexGroupingKeys, List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<AggregateFunction> edgeAggregateFunctions) {
    if (edgeGroupingKeys == null || edgeGroupingKeys.isEmpty()) {
      throw new IllegalArgumentException("Missing edge grouping key(s).");
    }

    return callForCollection(new EdgeRollUp(vertexGroupingKeys, vertexAggregateFunctions,
      edgeGroupingKeys, edgeAggregateFunctions));
  }

  @Override
  public LogicalGraph verify() {
    return callForGraph(new Verify<>());
  }

  @Override
  public LogicalGraph verifyGraphContainment() {
    return callForGraph(new VerifyGraphContainment<>());
  }

  //----------------------------------------------------------------------------
  // Binary Operators
  //----------------------------------------------------------------------------

  @Override
  public LogicalGraph combine(LogicalGraph otherGraph) {
    return callForGraph(new Combination(), otherGraph);
  }

  @Override
  public LogicalGraph overlap(LogicalGraph otherGraph) {
    return callForGraph(new Overlap(), otherGraph);
  }

  @Override
  public LogicalGraph exclude(LogicalGraph otherGraph) {
    return callForGraph(new Exclusion(), otherGraph);
  }

  @Override
  public DataSet<Boolean> equalsByElementIds(LogicalGraph other) {
    return new GraphEquality(
      new GraphHeadToEmptyString(),
      new VertexToIdString(),
      new EdgeToIdString(), true).execute(this, other);
  }

  @Override
  public DataSet<Boolean> equalsByElementData(LogicalGraph other) {
    return new GraphEquality(
      new GraphHeadToEmptyString(),
      new VertexToDataString(),
      new EdgeToDataString(), true).execute(this, other);
  }

  @Override
  public DataSet<Boolean> equalsByData(LogicalGraph other) {
    return new GraphEquality(
      new GraphHeadToDataString(),
      new VertexToDataString(),
      new EdgeToDataString(), true).execute(this, other);
  }

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  @Override
  public LogicalGraph callForGraph(UnaryBaseGraphToBaseGraphOperator<LogicalGraph> operator) {
    return operator.execute(this);
  }

  @Override
  public LogicalGraph callForGraph(BinaryGraphToGraphOperator operator, LogicalGraph otherGraph) {
    return operator.execute(this, otherGraph);
  }

  @Override
  public LogicalGraph callForGraph(GraphsToGraphOperator operator,
    LogicalGraph... otherGraphs) {
    return operator.execute(this, otherGraphs);
  }

  @Override
  public GraphCollection callForCollection(UnaryGraphToCollectionOperator operator) {
    return operator.execute(this);
  }

  @Override
  public GraphCollection splitBy(String propertyKey) {
    return callForCollection(new Split(new PropertyGetter<>(Lists.newArrayList(propertyKey))));
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  @Override
  public DataSet<Boolean> isEmpty() {
    return getVertices()
      .map(new True<>())
      .distinct()
      .union(getConfig().getExecutionEnvironment().fromElements(false))
      .reduce(new Or())
      .map(new Not());
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
   * Prints the GDL formatted graph to the standard output.
   *
   * @throws Exception forwarded from dataset print
   */
  public void print() throws Exception {
    GDLConsoleOutput.print(this);
  }
}
