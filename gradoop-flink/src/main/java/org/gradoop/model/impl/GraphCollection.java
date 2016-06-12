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

package org.gradoop.model.impl;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.io.api.DataSink;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.ApplicableUnaryGraphToGraphOperator;
import org.gradoop.model.api.operators.BinaryCollectionToCollectionOperator;
import org.gradoop.model.api.operators.GraphCollectionOperators;
import org.gradoop.model.api.operators.ReducibleBinaryGraphToGraphOperator;
import org.gradoop.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.api.operators.UnaryCollectionToGraphOperator;
import org.gradoop.model.impl.functions.bool.Not;
import org.gradoop.model.impl.functions.bool.Or;
import org.gradoop.model.impl.functions.bool.True;
import org.gradoop.model.impl.functions.epgm.BySameId;
import org.gradoop.model.impl.functions.epgm.GraphElementExpander;
import org.gradoop.model.impl.functions.epgm.GraphElementSet;
import org.gradoop.model.impl.functions.epgm.GraphTransactionTriple;
import org.gradoop.model.impl.functions.epgm.GraphVerticesEdges;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.epgm.TransactionEdges;
import org.gradoop.model.impl.functions.epgm.TransactionGraphHead;
import org.gradoop.model.impl.functions.epgm.TransactionVertices;
import org.gradoop.model.impl.functions.graphcontainment.InAnyGraph;
import org.gradoop.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.model.impl.functions.utils.First;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.operators.difference.Difference;
import org.gradoop.model.impl.operators.difference.DifferenceBroadcast;
import org.gradoop.model.impl.operators.distinct.Distinct;
import org.gradoop.model.impl.operators.equality.CollectionEquality;
import org.gradoop.model.impl.operators.equality.CollectionEqualityByGraphIds;
import org.gradoop.model.impl.operators.intersection.Intersection;
import org.gradoop.model.impl.operators.intersection.IntersectionBroadcast;
import org.gradoop.model.impl.operators.limit.Limit;
import org.gradoop.model.impl.operators.selection.Selection;
import org.gradoop.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.model.impl.operators.tostring.functions.EdgeToIdString;
import org.gradoop.model.impl.operators.tostring.functions
  .GraphHeadToDataString;
import org.gradoop.model.impl.operators.tostring.functions
  .GraphHeadToEmptyString;
import org.gradoop.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.model.impl.operators.tostring.functions.VertexToIdString;
import org.gradoop.model.impl.operators.union.Union;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.gradoop.util.GradoopFlinkConfig;
import org.gradoop.util.Order;

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
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GraphCollection
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends GraphBase<G, V, E>
  implements GraphCollectionOperators<G, V, E> {

  /**
   * Creates a graph collection from the given arguments.
   *
   * @param graphHeads  graph heads
   * @param vertices    vertices
   * @param edges       edges
   * @param config      Gradoop Flink configuration
   */
  private GraphCollection(DataSet<G> graphHeads,
    DataSet<V> vertices,
    DataSet<E> edges,
    GradoopFlinkConfig<G, V, E> config) {
    super(graphHeads, vertices, edges, config);
  }

  //----------------------------------------------------------------------------
  // Factory methods
  //----------------------------------------------------------------------------

  /**
   * Creates an empty graph collection.
   *
   * @param config  Gradoop Flink configuration
   * @param <G>     EPGM graph head type
   * @param <V>     EPGM vertex type
   * @param <E>     EPGM edge type
   * @return empty graph collection
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  GraphCollection<G, V, E> createEmptyCollection(
    GradoopFlinkConfig<G, V, E> config) {
    Collection<G> graphHeads = new ArrayList<>();
    Collection<V> vertices = new ArrayList<>();
    Collection<E> edges = new ArrayList<>();

    return GraphCollection.fromCollections(graphHeads, vertices, edges, config);
  }

  /**
   * Creates a graph collection from the given arguments.
   *
   * @param graphHeads  GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @param config      Gradoop Flink configuration
   * @param <G>         EPGM graph head type
   * @param <V>         EPGM vertex type
   * @param <E>         EPGM edge type
   * @return Graph collection
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  GraphCollection<G, V, E>
  fromDataSets(DataSet<G> graphHeads, DataSet<V> vertices,
    GradoopFlinkConfig<G, V, E> config) {
    return fromDataSets(
      graphHeads,
      vertices,
      createEdgeDataSet(new ArrayList<E>(0), config),
      config
    );
  }

  /**
   * Creates a graph collection from the given arguments.
   *
   * @param graphHeads  GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @param edges       Edge DataSet
   * @param config      Gradoop Flink configuration
   * @param <G>         EPGM graph head type
   * @param <V>         EPGM vertex type
   * @param <E>         EPGM edge type
   * @return Graph collection
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  GraphCollection<G, V, E>
  fromDataSets(DataSet<G> graphHeads, DataSet<V> vertices, DataSet<E> edges,
    GradoopFlinkConfig<G, V, E> config) {

    checkNotNull(graphHeads, "GraphHead DataSet was null");
    checkNotNull(vertices, "Vertex DataSet was null");
    checkNotNull(edges, "Edge DataSet was null");
    checkNotNull(config, "Config was null");
    return new GraphCollection<>(graphHeads, vertices, edges, config);
  }

  /**
   * Creates a new graph collection from the given collection.
   *
   * @param graphHeads  Graph Head collection
   * @param vertices    Vertex collection
   * @param edges       Edge collection
   * @param config      Gradoop Flink configuration
   * @param <G>         EPGM graph type
   * @param <V>         EPGM vertex type
   * @param <E>         EPGM edge type
   * @return Graph collection
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>

  GraphCollection<G, V, E> fromCollections(
    Collection<G> graphHeads,
    Collection<V> vertices,
    Collection<E> edges,
    GradoopFlinkConfig<G, V, E> config) {

    checkNotNull(graphHeads, "GraphHead collection was null");
    checkNotNull(vertices, "Vertex collection was null");
    checkNotNull(edges, "Vertex collection was null");
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
   * @param <G>           EPGM graph type
   * @param <V>           EPGM vertex type
   * @param <E>           EPGM edge type
   * @return 1-element graph collection
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  GraphCollection<G, V, E> fromGraph(LogicalGraph<G, V, E> logicalGraph) {
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
  public DataSet<G> getGraphHeads() {
    return this.graphHeads;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> getGraph(final GradoopId graphID) {
    // filter vertices and edges based on given graph id
    DataSet<G> graphHead = getGraphHeads()
      .filter(new BySameId<G>(graphID));

    DataSet<V> vertices = getVertices()
      .filter(new InGraph<V>(graphID));
    DataSet<E> edges = getEdges()
      .filter(new InGraph<E>(graphID));

    return LogicalGraph.fromDataSets(graphHead, vertices, edges, getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> getGraphs(final GradoopId... identifiers) {

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
  public GraphCollection<G, V, E> getGraphs(final GradoopIdSet identifiers) {

    DataSet<G> newGraphHeads =
      this.graphHeads.filter(new FilterFunction<G>() {

        @Override
        public boolean filter(G graphHead) throws Exception {
          return identifiers.contains(graphHead.getId());

        }
      });

    // build new vertex set
    DataSet<V> vertices = getVertices()
      .filter(new InAnyGraph<V>(identifiers));

    // build new edge set
    DataSet<E> edges = getEdges()
      .filter(new InAnyGraph<E>(identifiers));

    return new GraphCollection<>(newGraphHeads, vertices, edges, getConfig());
  }

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> select(final FilterFunction<G> predicate) {
    return callForCollection(new Selection<G, V, E>(predicate));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> distinct() {
    return callForCollection(new Distinct<G, V, E>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> sortBy(String propertyKey, Order order) {
    throw new NotImplementedException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> limit(int n) {
    return callForCollection(new Limit<G, V, E>(n));
  }

  //----------------------------------------------------------------------------
  // Binary Operators
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> union(
    GraphCollection<G, V, E> otherCollection) {
    return callForCollection(new Union<G, V, E>(), otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> intersect(
    GraphCollection<G, V, E> otherCollection) {
    return callForCollection(new Intersection<G, V, E>(), otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> intersectWithSmallResult(
    GraphCollection<G, V, E> otherCollection) {
    return callForCollection(new IntersectionBroadcast<G, V, E>(),
      otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> difference(
    GraphCollection<G, V, E> otherCollection) {
    return callForCollection(new Difference<G, V, E>(), otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> differenceWithSmallResult(
    GraphCollection<G, V, E> otherCollection) {
    return callForCollection(new DifferenceBroadcast<G, V, E>(),
      otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByGraphIds(GraphCollection<G, V, E> other) {
    return new CollectionEqualityByGraphIds<G, V, E>().execute(this, other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByGraphElementIds(
    GraphCollection<G, V, E> other) {
    return new CollectionEquality<>(
      new GraphHeadToEmptyString<G>(),
      new VertexToIdString<V>(),
      new EdgeToIdString<E>(), true).execute(this, other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByGraphElementData(
    GraphCollection<G, V, E> other) {
    return new CollectionEquality<>(
      new GraphHeadToEmptyString<G>(),
      new VertexToDataString<V>(),
      new EdgeToDataString<E>(), true).execute(this, other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByGraphData(GraphCollection<G, V, E> other) {
    return new CollectionEquality<>(
      new GraphHeadToDataString<G>(),
      new VertexToDataString<V>(),
      new EdgeToDataString<E>(), true).execute(this, other);
  }

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> callForCollection(
    UnaryCollectionToCollectionOperator<G, V, E> op) {
    return op.execute(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> callForCollection(
    BinaryCollectionToCollectionOperator<G, V, E> op,
    GraphCollection<G, V, E> otherCollection) {
    return op.execute(this, otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> callForGraph(
    UnaryCollectionToGraphOperator<G, V, E> op) {
    return op.execute(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> apply(
    ApplicableUnaryGraphToGraphOperator<G, V, E> op) {
    return callForCollection(op);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> reduce(
    ReducibleBinaryGraphToGraphOperator<G, V, E> op) {
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
      .map(new True<G>())
      .distinct()
      .union(getConfig().getExecutionEnvironment().fromElements(false))
      .reduce(new Or())
      .map(new Not());
  }

  /**
   * Creates a graph collection from a graph transaction dataset.
   * Overlapping vertices and edge are merged by Id comparison only.
   *
   * @param <G>           EPGM graph head type
   * @param <V>           EPGM vertex type
   * @param <E>           EPGM edge type
   * @param transactions  transaction dataset
   * @return graph collection
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  GraphCollection<G, V, E> fromTransactions(
    GraphTransactions<G, V, E> transactions) {

    GroupReduceFunction<V, V> vertexReducer = new First<>();
    GroupReduceFunction<E, E> edgeReducer = new First<>();

    return fromTransactions(transactions, vertexReducer, edgeReducer);
  }

  /**
   * Creates a graph collection from a graph transaction dataset.
   * Overlapping vertices and edge are merged using provided reduce functions.
   *
   * @param <G>                 EPGM graph head type
   * @param <V>                 EPGM vertex type
   * @param <E>                 EPGM edge type
   * @param transactions        transaction dataset
   * @param vertexMergeReducer  vertex merge function
   * @param edgeMergeReducer    edge merge function
   * @return graph collection
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  GraphCollection<G, V, E> fromTransactions(
    GraphTransactions<G, V, E> transactions,
    GroupReduceFunction<V, V> vertexMergeReducer,
    GroupReduceFunction<E, E> edgeMergeReducer) {

    GradoopFlinkConfig<G, V, E> config = transactions.getConfig();

    DataSet<Tuple3<G, Set<V>, Set<E>>> triples = transactions
      .getTransactions()
      .map(new GraphTransactionTriple<G, V, E>());

    DataSet<G> graphHeads =
      triples.map(new TransactionGraphHead<G, V, E>());

    DataSet<V> vertices = triples
      .flatMap(new TransactionVertices<G, V, E>())
      .returns(config.getVertexFactory().getType())
      .groupBy(new Id<V>())
      .reduceGroup(vertexMergeReducer);

    DataSet<E> edges = triples
      .flatMap(new TransactionEdges<G, V, E>())
      .returns(config.getEdgeFactory().getType())
      .groupBy(new Id<E>())
      .reduceGroup(edgeMergeReducer);

    return fromDataSets(graphHeads, vertices, edges, config);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphTransactions<G, V, E> toTransactions() {

    DataSet<Tuple2<GradoopId, Set<V>>> graphVertices = getVertices()
      .flatMap(new GraphElementExpander<V>())
      .groupBy(0)
      .reduceGroup(new GraphElementSet<V>());

    DataSet<Tuple2<GradoopId, Set<E>>> graphEdges = getEdges()
      .flatMap(new GraphElementExpander<E>())
      .groupBy(0)
      .reduceGroup(new GraphElementSet<E>());

    DataSet<GraphTransaction<G, V, E>>  transactions = graphVertices
      .join(graphEdges)
      .where(0).equalTo(0)
      .with(new GraphVerticesEdges<V, E>())
      .join(getGraphHeads()).where(0)
      .equalTo(new Id<G>())
      .with(new GraphTransactionTriple<G, V, E>());
    return new GraphTransactions<>(transactions, getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeTo(DataSink<G, V, E> dataSink) throws IOException {
    dataSink.write(this);
  }
}
