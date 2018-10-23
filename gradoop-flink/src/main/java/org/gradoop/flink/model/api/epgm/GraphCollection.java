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
package org.gradoop.flink.model.api.epgm;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.util.Order;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.gdl.GDLConsoleOutput;
import org.gradoop.flink.model.api.functions.GraphHeadReduceFunction;
import org.gradoop.flink.model.api.functions.timeextractors.EdgeTimeIntervalExtractor;
import org.gradoop.flink.model.api.functions.timeextractors.EdgeTimestampExtractor;
import org.gradoop.flink.model.api.functions.timeextractors.GraphHeadTimeIntervalExtractor;
import org.gradoop.flink.model.api.functions.timeextractors.GraphHeadTimestampExtractor;
import org.gradoop.flink.model.api.functions.timeextractors.VertexTimeIntervalExtractor;
import org.gradoop.flink.model.api.functions.timeextractors.VertexTimestampExtractor;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.operators.ApplicableUnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.BinaryCollectionToCollectionOperator;
import org.gradoop.flink.model.api.operators.ReducibleBinaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.api.operators.UnaryCollectionToGraphOperator;
import org.gradoop.flink.model.api.tpgm.TemporalGraphCollection;
import org.gradoop.flink.model.impl.epgm.EPGMGraphCollection;
import org.gradoop.flink.model.impl.epgm.EPGMLogicalGraph;
import org.gradoop.flink.model.impl.functions.bool.Not;
import org.gradoop.flink.model.impl.functions.bool.Or;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.functions.epgm.BySameId;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAnyGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraph;
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

import java.io.IOException;
import java.util.Collections;

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
public interface GraphCollection extends GraphCollectionOperators, GraphCollectionLayout {

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  @Override
  default GraphCollection select(final FilterFunction<GraphHead> predicate) {
    return callForCollection(new Selection(predicate));
  }

  @Override
  default GraphCollection sortBy(String propertyKey, Order order) {
    throw new NotImplementedException();
  }

  @Override
  default GraphCollection limit(int n) {
    return callForCollection(new Limit(n));
  }

  @Override
  default GraphCollection match(String pattern, PatternMatchingAlgorithm algorithm,
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
  default GraphCollection union(GraphCollection otherCollection) {
    return callForCollection(new Union(), otherCollection);
  }

  @Override
  default GraphCollection intersect(GraphCollection otherCollection) {
    return callForCollection(new Intersection(), otherCollection);
  }

  @Override
  default GraphCollection intersectWithSmallResult(
    GraphCollection otherCollection) {
    return callForCollection(new IntersectionBroadcast(),
      otherCollection);
  }

  @Override
  default GraphCollection difference(GraphCollection otherCollection) {
    return callForCollection(new Difference(), otherCollection);
  }

  @Override
  default GraphCollection differenceWithSmallResult(
    GraphCollection otherCollection) {
    return callForCollection(new DifferenceBroadcast(),
      otherCollection);
  }

  @Override
  default DataSet<Boolean> equalsByGraphIds(GraphCollection other) {
    return new CollectionEqualityByGraphIds().execute(this, other);
  }

  @Override
  default DataSet<Boolean> equalsByGraphElementIds(GraphCollection other) {
    return new CollectionEquality(
      new GraphHeadToEmptyString(),
      new VertexToIdString(),
      new EdgeToIdString(), true).execute(this, other);
  }

  @Override
  default DataSet<Boolean> equalsByGraphElementData(GraphCollection other) {
    return new CollectionEquality(
      new GraphHeadToEmptyString(),
      new VertexToDataString(),
      new EdgeToDataString(), true).execute(this, other);
  }

  @Override
  default DataSet<Boolean> equalsByGraphData(GraphCollection other) {
    return new CollectionEquality(
      new GraphHeadToDataString(),
      new VertexToDataString(),
      new EdgeToDataString(), true).execute(this, other);
  }

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  @Override
  default GraphCollection callForCollection(UnaryCollectionToCollectionOperator op) {
    return op.execute(this);
  }

  @Override
  default GraphCollection callForCollection(BinaryCollectionToCollectionOperator op,
    GraphCollection otherCollection) {
    return op.execute(this, otherCollection);
  }

  @Override
  default LogicalGraph callForGraph(UnaryCollectionToGraphOperator op) {
    return op.execute(this);
  }

  @Override
  default GraphCollection apply(ApplicableUnaryGraphToGraphOperator op) {
    return callForCollection(op);
  }

  @Override
  default LogicalGraph reduce(ReducibleBinaryGraphToGraphOperator op) {
    return callForGraph(op);
  }

  //----------------------------------------------------------------------------
  // Logical Graph / Graph Head Getters
  //----------------------------------------------------------------------------

  @Override
  default LogicalGraph getGraph(final GradoopId graphID) {
    // filter vertices and edges based on given graph id
    DataSet<GraphHead> graphHead = getGraphHeads()
      .filter(new BySameId<>(graphID));
    DataSet<Vertex> vertices = getVertices()
      .filter(new InGraph<>(graphID));
    DataSet<Edge> edges = getEdges()
      .filter(new InGraph<>(graphID));

    return new EPGMLogicalGraph(
      getConfig().getLogicalGraphFactory().fromDataSets(graphHead, vertices, edges), getConfig());
  }

  @Override
  default GraphCollection getGraphs(final GradoopId... identifiers) {
    GradoopIdSet graphIds = new GradoopIdSet();
    Collections.addAll(graphIds, identifiers);
    return getGraphs(graphIds);
  }

  @Override
  default GraphCollection getGraphs(final GradoopIdSet identifiers) {
    DataSet<GraphHead> newGraphHeads = this.getGraphHeads()
      .filter((FilterFunction<GraphHead>) graphHead -> identifiers.contains(graphHead.getId()));

    // build new vertex set
    DataSet<Vertex> vertices = getVertices()
      .filter(new InAnyGraph<>(identifiers));

    // build new edge set
    DataSet<Edge> edges = getEdges()
      .filter(new InAnyGraph<>(identifiers));

    return new EPGMGraphCollection(
      getConfig().getGraphCollectionFactory().fromDataSets(newGraphHeads, vertices, edges),
      getConfig());
  }

  /**
   * Converts the {@link GraphCollection} to a {@link TemporalGraphCollection} instance. Since there
   * is no extractor function provided for this function, the valid times of all elements will be
   * empty.
   *
   * @return the graph collection represented as temporal graph collection with empty valid time
   * attributes
   */
  default TemporalGraphCollection toTemporalGraph() {
    return getConfig().getTemporalGraphCollectionFactory()
      .fromNonTemporalDataSets(getVertices(), getEdges(), getGraphHeads());
  }

  /**
   * Converts the {@link GraphCollection} to a {@link TemporalGraphCollection} instance.
   * By the provided timestamp extractors, it is possible to extract a temporal information from the
   * data to define a timestamp that represents the beginning of the element's validity
   * (valid time).
   * The value of the validTo property remains a default value.
   * Use {@link GraphCollection#toTemporalGraph(GraphHeadTimeIntervalExtractor,
   * VertexTimeIntervalExtractor, EdgeTimeIntervalExtractor)} to define the beginning and the end
   * of the element's validity.
   *
   * @param graphHeadTimestampExtractor extractor function to pick the timestamp from graph heads
   * @param vertexTimestampExtractor extractor function to pick the timestamp from vertices
   * @param edgeTimestampExtractor extractor function to pick the timestamp from edges
   * @return the graph collection represented as temporal graph collection with a timestamp as
   * validFrom attribute
   */
  default TemporalGraphCollection toTemporalGraph(
    GraphHeadTimestampExtractor graphHeadTimestampExtractor,
    VertexTimestampExtractor vertexTimestampExtractor,
    EdgeTimestampExtractor edgeTimestampExtractor) {

    return getConfig().getTemporalGraphCollectionFactory().fromDataSets(
      getVertices().map(vertexTimestampExtractor),
      getEdges().map(edgeTimestampExtractor),
      getGraphHeads().map(graphHeadTimestampExtractor));
  }

  /**
   * Converts the {@link GraphCollection} to a {@link TemporalGraphCollection} instance.
   * By the provided timestamp extractors, it is possible to extract temporal information from the
   * data to define a time interval that represents the beginning and end of the element's validity
   * (valid time).
   * Use {@link GraphCollection#toTemporalGraph(GraphHeadTimestampExtractor,
   * VertexTimestampExtractor, EdgeTimestampExtractor)} to define only a timestamp as the
   * beginning of the element's validity.
   *
   * @param graphHeadTimeIntervalExtractor extractor to pick the time interval from graph heads
   * @param vertexTimeIntervalExtractor extractor to pick the time interval from vertices
   * @param edgeTimeIntervalExtractor extractor to pick the time interval from edges
   * @return the graph collection represented as temporal graph collection with a time interval as
   * valid time
   */
  default TemporalGraphCollection toTemporalGraph(
    GraphHeadTimeIntervalExtractor graphHeadTimeIntervalExtractor,
    VertexTimeIntervalExtractor vertexTimeIntervalExtractor,
    EdgeTimeIntervalExtractor edgeTimeIntervalExtractor) {

    return getConfig().getTemporalGraphCollectionFactory().fromDataSets(
      getVertices().map(vertexTimeIntervalExtractor),
      getEdges().map(edgeTimeIntervalExtractor),
      getGraphHeads().map(graphHeadTimeIntervalExtractor));
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  @Override
  default DataSet<Boolean> isEmpty() {
    return getGraphHeads()
      .map(new True<>())
      .distinct()
      .union(getConfig().getExecutionEnvironment().fromElements(false))
      .reduce(new Or())
      .map(new Not());
  }

  @Override
  default GraphCollection distinctById() {
    return callForCollection(new DistinctById());
  }

  @Override
  default GraphCollection distinctByIsomorphism() {
    return callForCollection(new DistinctByIsomorphism());
  }

  @Override
  default GraphCollection groupByIsomorphism(GraphHeadReduceFunction func) {
    return callForCollection(new GroupByIsomorphism(func));
  }

  @Override
  default void writeTo(DataSink dataSink) throws IOException {
    dataSink.write(this);
  }

  @Override
  default void writeTo(DataSink dataSink, boolean overWrite) throws IOException {
    dataSink.write(this, overWrite);
  }

  /**
   * Prints this graph collection to the console.
   *
   * @throws Exception forwarded DataSet print() Exception.
   */
  default void print() throws Exception {
    GDLConsoleOutput.print(this);
  }

}
