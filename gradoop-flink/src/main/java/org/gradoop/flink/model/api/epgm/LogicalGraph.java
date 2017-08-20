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
package org.gradoop.flink.model.api.epgm;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.bool.Not;
import org.gradoop.flink.model.impl.functions.bool.Or;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.functions.epgm.PropertyGetter;
import org.gradoop.flink.model.impl.operators.aggregation.Aggregation;
import org.gradoop.flink.model.impl.operators.cloning.Cloning;
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.model.impl.operators.drilling.Drill;
import org.gradoop.flink.model.impl.operators.drilling.functions.drillfunctions.DrillFunction;
import org.gradoop.flink.model.impl.operators.equality.GraphEquality;
import org.gradoop.flink.model.impl.operators.exclusion.Exclusion;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.DFSTraverser;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.CypherPatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.ExplorativePatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.TraverserStrategy;
import org.gradoop.flink.model.impl.operators.neighborhood.Neighborhood;
import org.gradoop.flink.model.impl.operators.neighborhood.ReduceEdgeNeighborhood;
import org.gradoop.flink.model.impl.operators.neighborhood.ReduceVertexNeighborhood;
import org.gradoop.flink.model.impl.operators.overlap.Overlap;
import org.gradoop.flink.model.impl.operators.sampling.RandomNodeSampling;
import org.gradoop.flink.model.impl.operators.split.Split;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToIdString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToIdString;
import org.gradoop.flink.model.impl.operators.transformation.Transformation;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A logical graph is one of the base concepts of the Extended Property Graph Model. A logical graph
 * encapsulates three concepts:
 *
 * - a so-called graph head, that stores information about the graph (i.e. label and properties)
 * - a set of vertices assigned to the graph
 * - a set of directed, possibly parallel edges assigned to the graph
 *
 * Furthermore, a logical graph provides operations that are performed on the underlying data. These
 * operations result in either another logical graph or in a {@link GraphCollection}.
 *
 * A logical graph is wrapping a {@link LogicalGraphLayout} which defines, how the graph is
 * represented in Apache Flink. Note that the LogicalGraph also implements that interface and
 * just forward the calls to the layout. This is just for convenience and API synchronicity.
 */
public class LogicalGraph implements LogicalGraphLayout, LogicalGraphOperators {
  /**
   * Layout for that logical graph.
   */
  private final LogicalGraphLayout layout;
  /**
   * Configuration
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a new logical graph based on the given parameters.
   *
   * @param layout representation of the logical graph
   * @param config Gradoop Flink configuration
   */
  LogicalGraph(LogicalGraphLayout layout, GradoopFlinkConfig config) {
    Objects.requireNonNull(layout);
    Objects.requireNonNull(config);
    this.layout = layout;
    this.config = config;
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

  /**
   * {@inheritDoc}
   */
  public DataSet<GraphHead> getGraphHead() {
    return layout.getGraphHead();
  }

  @Override
  public GradoopFlinkConfig getConfig() {
    return config;
  }

  @Override
  public DataSet<Vertex> getVertices() {
    return layout.getVertices();
  }

  @Override
  public DataSet<Vertex> getVerticesByLabel(String label) {
    return layout.getVerticesByLabel(label);
  }

  @Override
  public DataSet<Edge> getEdges() {
    return layout.getEdges();
  }

  @Override
  public DataSet<Edge> getEdgesByLabel(String label) {
    return layout.getEdgesByLabel(label);
  }

  @Override
  public DataSet<Edge> getOutgoingEdges(GradoopId vertexID) {
    return layout.getOutgoingEdges(vertexID);
  }

  @Override
  public DataSet<Edge> getIncomingEdges(GradoopId vertexID) {
    return layout.getIncomingEdges(vertexID);
  }

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection cypher(String query) {
    return cypher(query, new GraphStatistics(1, 1, 1, 1));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection cypher(String query, GraphStatistics graphStatistics) {
    return cypher(query, true,
      MatchStrategy.HOMOMORPHISM, MatchStrategy.ISOMORPHISM, graphStatistics);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection cypher(String query, boolean attachData, MatchStrategy vertexStrategy,
    MatchStrategy edgeStrategy, GraphStatistics graphStatistics) {
    return callForCollection(new CypherPatternMatching(query, attachData,
      vertexStrategy, edgeStrategy, graphStatistics));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection match(String pattern) {
    return match(pattern, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection match(String pattern, boolean attachData) {
    return match(pattern, attachData, MatchStrategy.ISOMORPHISM,
      TraverserStrategy.SET_PAIR_BULK_ITERATION);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection match(String pattern, boolean attachData,
    MatchStrategy matchStrategy, TraverserStrategy traverserStrategy) {

    ExplorativePatternMatching op = new ExplorativePatternMatching.Builder()
      .setQuery(pattern)
      .setAttachData(attachData)
      .setMatchStrategy(matchStrategy)
      .setTraverserStrategy(traverserStrategy)
      .setTraverser(new DFSTraverser()).build();

    return callForCollection(op);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph copy() {
    return callForGraph(new Cloning());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph transform(
    TransformationFunction<GraphHead> graphHeadTransformationFunction,
    TransformationFunction<Vertex> vertexTransformationFunction,
    TransformationFunction<Edge> edgeTransformationFunction) {
    return callForGraph(new Transformation(
      graphHeadTransformationFunction,
      vertexTransformationFunction,
      edgeTransformationFunction));
  }

  @Override
  public LogicalGraph transformGraphHead(
    TransformationFunction<GraphHead> graphHeadTransformationFunction) {
    return transform(graphHeadTransformationFunction, null, null);
  }

  @Override
  public LogicalGraph transformVertices(
    TransformationFunction<Vertex> vertexTransformationFunction) {
    return transform(null, vertexTransformationFunction, null);
  }

  @Override
  public LogicalGraph transformEdges(
    TransformationFunction<Edge> edgeTransformationFunction) {
    return transform(null, null, edgeTransformationFunction);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph vertexInducedSubgraph(
    FilterFunction<Vertex> vertexFilterFunction) {
    Objects.requireNonNull(vertexFilterFunction);
    return callForGraph(new Subgraph(vertexFilterFunction, null));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph edgeInducedSubgraph(
    FilterFunction<Edge> edgeFilterFunction) {
    Objects.requireNonNull(edgeFilterFunction);
    return callForGraph(new Subgraph(null, edgeFilterFunction));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph subgraph(FilterFunction<Vertex> vertexFilterFunction,
    FilterFunction<Edge> edgeFilterFunction) {
    Objects.requireNonNull(vertexFilterFunction);
    Objects.requireNonNull(edgeFilterFunction);
    return callForGraph(
      new Subgraph(vertexFilterFunction, edgeFilterFunction));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph aggregate(AggregateFunction aggregateFunc) {
    return callForGraph(new Aggregation(aggregateFunc));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph sampleRandomNodes(float sampleSize) {
    return callForGraph(new RandomNodeSampling(sampleSize));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph groupBy(List<String> vertexGroupingKeys) {
    return groupBy(vertexGroupingKeys, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph groupBy(List<String> vertexGroupingKeys, List<String> edgeGroupingKeys) {
    return groupBy(vertexGroupingKeys, null, edgeGroupingKeys, null, GroupingStrategy.GROUP_REDUCE);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph groupBy(
    List<String> vertexGroupingKeys, List<PropertyValueAggregator> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<PropertyValueAggregator> edgeAggregateFunctions,
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
      vertexAggregateFunctions.forEach(builder::addVertexAggregator);
    }
    if (edgeAggregateFunctions != null) {
      edgeAggregateFunctions.forEach(builder::addEdgeAggregator);
    }
    return callForGraph(builder.build());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph reduceOnEdges(
    EdgeAggregateFunction function, Neighborhood.EdgeDirection edgeDirection) {
    return callForGraph(new ReduceEdgeNeighborhood(function, edgeDirection));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph reduceOnNeighbors(
    VertexAggregateFunction function, Neighborhood.EdgeDirection edgeDirection) {
    return callForGraph(new ReduceVertexNeighborhood(function, edgeDirection));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph drillUpVertex(String propertyKey, DrillFunction function) {
    return drillUpVertex(null, propertyKey, function);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph drillUpVertex(
    String vertexLabel, String propertyKey, DrillFunction function) {
    return drillUpVertex(vertexLabel, propertyKey, function, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph drillUpVertex(
    String vertexLabel, String propertyKey, DrillFunction function, String newPropertyKey) {

    Objects.requireNonNull(propertyKey, "missing property key");
    Objects.requireNonNull(function, "missing drill function");

    Drill.DrillBuilder builder = new Drill.DrillBuilder();

    builder.setPropertyKey(propertyKey);
    builder.setFunction(function);
    builder.drillVertex(true);

    if (vertexLabel != null) {
      builder.setLabel(vertexLabel);
    }
    if (newPropertyKey != null) {
      builder.setNewPropertyKey(newPropertyKey);
    }

    return callForGraph(builder.buildDrillUp());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph drillUpEdge(String propertyKey, DrillFunction function) {
    return drillUpEdge(null, propertyKey, function);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph drillUpEdge(String edgeLabel, String propertyKey, DrillFunction function) {
    return drillUpEdge(edgeLabel, propertyKey, function, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph drillUpEdge(
    String edgeLabel, String propertyKey, DrillFunction function, String newPropertyKey) {

    Objects.requireNonNull(propertyKey, "missing property key");
    Objects.requireNonNull(function, "missing drill function");

    Drill.DrillBuilder builder = new Drill.DrillBuilder();

    builder.setPropertyKey(propertyKey);
    builder.setFunction(function);
    builder.drillEdge(true);

    if (edgeLabel != null) {
      builder.setLabel(edgeLabel);
    }
    if (newPropertyKey != null) {
      builder.setNewPropertyKey(newPropertyKey);
    }

    return callForGraph(builder.buildDrillUp());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph drillDownVertex(String propertyKey) {
    return drillDownVertex(null, propertyKey, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph drillDownVertex(String vertexLabel, String propertyKey) {
    return drillDownVertex(vertexLabel, propertyKey, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph drillDownVertex(String propertyKey, DrillFunction function) {
    return drillDownVertex(null, propertyKey, function);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph drillDownVertex(
    String vertexLabel, String propertyKey, DrillFunction function) {
    return drillDownVertex(vertexLabel, propertyKey, function, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph drillDownVertex(
    String vertexLabel, String propertyKey, DrillFunction function, String newPropertyKey) {

    Objects.requireNonNull(propertyKey, "missing property key");

    Drill.DrillBuilder builder = new Drill.DrillBuilder();

    builder.setPropertyKey(propertyKey);
    builder.setFunction(function);
    builder.drillVertex(true);

    if (vertexLabel != null) {
      builder.setLabel(vertexLabel);
    }
    if (newPropertyKey != null) {
      builder.setNewPropertyKey(newPropertyKey);
    }
    if (function != null) {
      builder.setFunction(function);
    }

    return callForGraph(builder.buildDrillDown());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph drillDownEdge(String propertyKey) {
    return drillDownEdge(null, propertyKey, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph drillDownEdge(String edgeLabel, String propertyKey) {
    return drillDownEdge(edgeLabel, propertyKey, null);
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph drillDownEdge(String propertyKey, DrillFunction function) {
    return drillDownEdge(null, propertyKey, function);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph drillDownEdge(String edgeLabel, String propertyKey, DrillFunction function) {
    return drillDownEdge(edgeLabel, propertyKey, function, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph drillDownEdge(
    String edgeLabel, String propertyKey, DrillFunction function, String newPropertyKey) {

    Objects.requireNonNull(propertyKey, "missing property key");

    Drill.DrillBuilder builder = new Drill.DrillBuilder();

    builder.setPropertyKey(propertyKey);
    builder.setFunction(function);
    builder.drillEdge(true);

    if (edgeLabel != null) {
      builder.setLabel(edgeLabel);
    }
    if (newPropertyKey != null) {
      builder.setNewPropertyKey(newPropertyKey);
    }
    if (function != null) {
      builder.setFunction(function);
    }

    return callForGraph(builder.buildDrillDown());
  }

  //----------------------------------------------------------------------------
  // Binary Operators
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph combine(LogicalGraph otherGraph) {
    return callForGraph(new Combination(), otherGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph overlap(LogicalGraph otherGraph) {
    return callForGraph(new Overlap(), otherGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph exclude(LogicalGraph otherGraph) {
    return callForGraph(new Exclusion(), otherGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByElementIds(LogicalGraph other) {
    return new GraphEquality(
      new GraphHeadToEmptyString(),
      new VertexToIdString(),
      new EdgeToIdString(), true).execute(this, other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByElementData(LogicalGraph other) {
    return new GraphEquality(
      new GraphHeadToEmptyString(),
      new VertexToDataString(),
      new EdgeToDataString(), true).execute(this, other);
  }

  /**
   * {@inheritDoc}
   */
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

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph callForGraph(UnaryGraphToGraphOperator operator) {
    return operator.execute(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph callForGraph(BinaryGraphToGraphOperator operator, LogicalGraph otherGraph) {
    return operator.execute(this, otherGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection callForCollection(UnaryGraphToCollectionOperator operator) {
    return operator.execute(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection splitBy(String propertyKey) {
    return callForCollection(new Split(new PropertyGetter<>(Lists.newArrayList(propertyKey))));
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> isEmpty() {
    return getVertices()
      .map(new True<>())
      .distinct()
      .union(getConfig().getExecutionEnvironment().fromElements(false))
      .reduce(new Or())
      .map(new Not());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeTo(DataSink dataSink) throws IOException {
    dataSink.write(this);
  }
}
