/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.api.operators.ApplicableUnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.BinaryCollectionToCollectionOperator;
import org.gradoop.flink.model.api.operators.GraphCollectionOperators;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.functions.bool.Not;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.functions.epgm.BySameId;
import org.gradoop.flink.model.impl.functions.epgm.GraphElementExpander;
import org.gradoop.flink.model.impl.functions.epgm.GraphElementsHeadsToTransaction;
import org.gradoop.flink.model.impl.functions.epgm.GraphTransactionTriple;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.TransactionEdges;
import org.gradoop.flink.model.impl.functions.epgm.TransactionGraphHead;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAnyGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.flink.model.impl.functions.utils.First;
import org.gradoop.flink.model.impl.operators.difference.Difference;
import org.gradoop.flink.model.impl.operators.difference.DifferenceBroadcast;
import org.gradoop.flink.model.impl.operators.equality.CollectionEquality;
import org.gradoop.flink.model.impl.operators.equality.CollectionEqualityByGraphIds;
import org.gradoop.flink.model.impl.operators.intersection.Intersection;
import org.gradoop.flink.model.impl.operators.intersection.IntersectionBroadcast;
import org.gradoop.flink.model.impl.operators.selection.Selection;
import org.gradoop.flink.model.impl.operators.tostring.functions
  .EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToIdString;
import org.gradoop.flink.model.impl.operators.tostring.functions
  .GraphHeadToEmptyString;
import org.gradoop.flink.model.impl.operators.tostring.functions
  .VertexToIdString;
import org.gradoop.flink.model.impl.operators.union.Union;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.flink.model.api.operators.ReducibleBinaryGraphToGraphOperator;

import org.gradoop.flink.model.api.operators.UnaryCollectionToGraphOperator;
import org.gradoop.flink.model.impl.functions.bool.Or;
import org.gradoop.flink.model.impl.functions.epgm.TransactionVertices;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.model.impl.operators.distinct.Distinct;
import org.gradoop.flink.model.impl.operators.limit.Limit;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.common.util.Order;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import static org.apache.flink.shaded.com.google.common.base.Preconditions
  .checkNotNull;

/**
 * Represents a collection of graphs inside the EPGM. As graphs may share
 * vertices and edges, the collections contains a single gelly graph
 * representing all subgraphs. Graph data is stored in an additional dataset.
 */
public class GraphCollection extends GraphBase
  implements GraphCollectionOperators {

  /**
   * Creates a graph collection from the given arguments.
   *
   * @param graphHeads  graph heads
   * @param vertices    vertices
   * @param edges       edges
   * @param config      Gradoop Flink configuration
   */
  private GraphCollection(DataSet<EPGMGraphHead> graphHeads,
    DataSet<EPGMVertex> vertices,
    DataSet<EPGMEdge> edges,
    GradoopFlinkConfig config) {
    super(graphHeads, vertices, edges, config);
  }

  //----------------------------------------------------------------------------
  // Factory methods
  //----------------------------------------------------------------------------

  /**
   * Creates an empty graph collection.
   *
   * @param config  Gradoop Flink configuration
   * @return empty graph collection
   */
  public static GraphCollection createEmptyCollection(
    GradoopFlinkConfig config) {
    Collection<EPGMGraphHead> graphHeads = new ArrayList<>();
    Collection<EPGMVertex> vertices = new ArrayList<>();
    Collection<EPGMEdge> edges = new ArrayList<>();

    return GraphCollection.fromCollections(graphHeads, vertices, edges, config);
  }

  /**
   * Creates a graph collection from the given arguments.
   *
   * @param graphHeads  EPGMGraphHead DataSet
   * @param vertices    EPGMVertex DataSet
   * @param config      Gradoop Flink configuration
   * @return Graph collection
   */
  public static GraphCollection fromDataSets(DataSet<EPGMGraphHead> graphHeads,
    DataSet<EPGMVertex> vertices, GradoopFlinkConfig config) {
    return fromDataSets(
      graphHeads,
      vertices,
      createEdgeDataSet(new ArrayList<EPGMEdge>(0), config),
      config
    );
  }

  /**
   * Creates a graph collection from the given arguments.
   *
   * @param graphHeads  EPGMGraphHead DataSet
   * @param vertices    EPGMVertex DataSet
   * @param edges       EPGMEdge DataSet
   * @param config      Gradoop Flink configuration
   * @return Graph collection
   */
  public static GraphCollection fromDataSets(DataSet<EPGMGraphHead> graphHeads,
    DataSet<EPGMVertex> vertices, DataSet<EPGMEdge> edges, GradoopFlinkConfig config) {

    checkNotNull(graphHeads, "EPGMGraphHead DataSet was null");
    checkNotNull(vertices, "EPGMVertex DataSet was null");
    checkNotNull(edges, "EPGMEdge DataSet was null");
    checkNotNull(config, "Config was null");
    return new GraphCollection(graphHeads, vertices, edges, config);
  }

  /**
   * Creates a new graph collection from the given collection.
   *
   * @param graphHeads  Graph Head collection
   * @param vertices    EPGMVertex collection
   * @param edges       EPGMEdge collection
   * @param config      Gradoop Flink configuration
   * @return Graph collection
   */
  public static GraphCollection fromCollections(
    Collection<EPGMGraphHead> graphHeads,
    Collection<EPGMVertex> vertices,
    Collection<EPGMEdge> edges,
    GradoopFlinkConfig config) {

    checkNotNull(graphHeads, "EPGMGraphHead collection was null");
    checkNotNull(vertices, "EPGMVertex collection was null");
    checkNotNull(edges, "EPGMVertex collection was null");
    checkNotNull(config, "Config was null");
    return fromDataSets(
      createGraphHeadDataSet(graphHeads, config),
      createVertexDataSet(vertices, config),
      createEdgeDataSet(edges, config),
      config
    );
  }

  /**
   * Creates a graph collection from a given logical graph.
   *
   * @param logicalGraph  input graph
   * @return 1-element graph collection
   */
  public static GraphCollection fromGraph(LogicalGraph logicalGraph) {
    return fromDataSets(
      logicalGraph.getGraphHead(),
      logicalGraph.getVertices(),
      logicalGraph.getEdges(),
      logicalGraph.getConfig()
    );
  }

  //----------------------------------------------------------------------------
  // Logical Graph / Graph Head Getters
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  public DataSet<EPGMGraphHead> getGraphHeads() {
    return this.graphHeads;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph getGraph(final GradoopId graphID) {
    // filter vertices and edges based on given graph id
    DataSet<EPGMGraphHead> graphHead = getGraphHeads()
      .filter(new BySameId<EPGMGraphHead>(graphID));

    DataSet<EPGMVertex> vertices = getVertices()
      .filter(new InGraph<EPGMVertex>(graphID));
    DataSet<EPGMEdge> edges = getEdges()
      .filter(new InGraph<EPGMEdge>(graphID));

    return LogicalGraph.fromDataSets(graphHead, vertices, edges, getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection getGraphs(final GradoopId... identifiers) {

    GradoopIdSet graphIds = new GradoopIdSet();

    for (GradoopId id : identifiers) {
      graphIds.add(id);
    }

    return getGraphs(graphIds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection getGraphs(final GradoopIdSet identifiers) {

    DataSet<EPGMGraphHead> newGraphHeads =
      this.graphHeads.filter(new FilterFunction<EPGMGraphHead>() {

        @Override
        public boolean filter(EPGMGraphHead graphHead) throws Exception {
          return identifiers.contains(graphHead.getId());

        }
      });

    // build new vertex set
    DataSet<EPGMVertex> vertices = getVertices()
      .filter(new InAnyGraph<EPGMVertex>(identifiers));

    // build new edge set
    DataSet<EPGMEdge> edges = getEdges()
      .filter(new InAnyGraph<EPGMEdge>(identifiers));

    return new GraphCollection(newGraphHeads, vertices, edges, getConfig());
  }

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection select(final FilterFunction<EPGMGraphHead> predicate) {
    return callForCollection(new Selection(predicate));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection distinct() {
    return callForCollection(new Distinct());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection sortBy(String propertyKey, Order order) {
    throw new NotImplementedException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection limit(int n) {
    return callForCollection(new Limit(n));
  }

  //----------------------------------------------------------------------------
  // Binary Operators
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection union(
    GraphCollection otherCollection) {
    return callForCollection(new Union(), otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection intersect(
    GraphCollection otherCollection) {
    return callForCollection(new Intersection(), otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection intersectWithSmallResult(
    GraphCollection otherCollection) {
    return callForCollection(new IntersectionBroadcast(),
      otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection difference(
    GraphCollection otherCollection) {
    return callForCollection(new Difference(), otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection differenceWithSmallResult(
    GraphCollection otherCollection) {
    return callForCollection(new DifferenceBroadcast(),
      otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByGraphIds(GraphCollection other) {
    return new CollectionEqualityByGraphIds().execute(this, other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByGraphElementIds(
    GraphCollection other) {
    return new CollectionEquality(
      new GraphHeadToEmptyString<>(),
      new VertexToIdString<>(),
      new EdgeToIdString<>(), true).execute(this, other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByGraphElementData(
    GraphCollection other) {
    return new CollectionEquality(
      new GraphHeadToEmptyString<>(),
      new VertexToDataString<>(),
      new EdgeToDataString<>(), true).execute(this, other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByGraphData(GraphCollection other) {
    return new CollectionEquality(
      new GraphHeadToDataString<>(),
      new VertexToDataString<>(),
      new EdgeToDataString<>(), true).execute(this, other);
  }

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection callForCollection(
    UnaryCollectionToCollectionOperator op) {
    return op.execute(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection callForCollection(
    BinaryCollectionToCollectionOperator op,
    GraphCollection otherCollection) {
    return op.execute(this, otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph callForGraph(
    UnaryCollectionToGraphOperator op) {
    return op.execute(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection apply(
    ApplicableUnaryGraphToGraphOperator op) {
    return callForCollection(op);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph reduce(
    ReducibleBinaryGraphToGraphOperator op) {
    return callForGraph(op);
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> isEmpty() {
    return getGraphHeads()
      .map(new True<EPGMGraphHead>())
      .distinct()
      .union(getConfig().getExecutionEnvironment().fromElements(false))
      .reduce(new Or())
      .map(new Not());
  }

  /**
   * Creates a graph collection from a graph transaction dataset.
   * Overlapping vertices and edge are merged by Id comparison only.
   *
   * @param transactions  transaction dataset
   * @return graph collection
   */
  public static GraphCollection fromTransactions(
    GraphTransactions transactions) {

    GroupReduceFunction<EPGMVertex, EPGMVertex> vertexReducer = new First<>();
    GroupReduceFunction<EPGMEdge, EPGMEdge> edgeReducer = new First<>();

    return fromTransactions(transactions, vertexReducer, edgeReducer);
  }

  /**
   * Creates a graph collection from a graph transaction dataset.
   * Overlapping vertices and edge are merged using provided reduce functions.
   *
   * @param transactions        transaction dataset
   * @param vertexMergeReducer  vertex merge function
   * @param edgeMergeReducer    edge merge function
   * @return graph collection
   */
  public static
  GraphCollection fromTransactions(
    GraphTransactions transactions,
    GroupReduceFunction<EPGMVertex, EPGMVertex> vertexMergeReducer,
    GroupReduceFunction<EPGMEdge, EPGMEdge> edgeMergeReducer) {

    GradoopFlinkConfig config = transactions.getConfig();

    DataSet<Tuple3<EPGMGraphHead, Set<EPGMVertex>, Set<EPGMEdge>>> triples = transactions
      .getTransactions()
      .map(new GraphTransactionTriple());

    DataSet<EPGMGraphHead> graphHeads = triples.map(new TransactionGraphHead());

    DataSet<EPGMVertex> vertices = triples
      .flatMap(new TransactionVertices())
//      .returns(config.getVertexFactory().getType())
      .groupBy(new Id<EPGMVertex>())
      .reduceGroup(vertexMergeReducer);

    DataSet<EPGMEdge> edges = triples
      .flatMap(new TransactionEdges())
//      .returns(config.getEdgeFactory().getType())
      .groupBy(new Id<EPGMEdge>())
      .reduceGroup(edgeMergeReducer);

    return fromDataSets(graphHeads, vertices, edges, config);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphTransactions toTransactions() {
    DataSet<Tuple2<GradoopId, EPGMGraphElement>> graphVertices = getVertices()
      .flatMap(new GraphElementExpander<EPGMVertex>());

    DataSet<Tuple2<GradoopId, EPGMGraphElement>> graphEdges = getEdges()
      .flatMap(new GraphElementExpander<EPGMEdge>());

    DataSet<Tuple2<GradoopId, EPGMGraphElement>> verticesAndEdges =
      graphVertices.union(graphEdges);

    DataSet<GraphTransaction>  transactions = verticesAndEdges
      .coGroup(getGraphHeads())
      .where(0).equalTo(new Id<EPGMGraphHead>())
      .with(new GraphElementsHeadsToTransaction());

    return new GraphTransactions(transactions, getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeTo(DataSink dataSink) throws IOException {
    dataSink.write(this);
  }
}
