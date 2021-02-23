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
package org.gradoop.flink.model.api.epgm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.model.api.functions.GraphHeadReduceFunction;
import org.gradoop.flink.model.api.operators.ApplicableUnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.operators.BinaryBaseGraphCollectionToBaseGraphCollectionOperator;
import org.gradoop.flink.model.api.operators.BinaryBaseGraphCollectionToValueOperator;
import org.gradoop.flink.model.api.operators.ReducibleBinaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphCollectionToBaseGraphCollectionOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphCollectionToBaseGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphCollectionToValueOperator;
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.model.impl.operators.difference.Difference;
import org.gradoop.flink.model.impl.operators.difference.DifferenceBroadcast;
import org.gradoop.flink.model.impl.operators.distinction.DistinctById;
import org.gradoop.flink.model.impl.operators.distinction.DistinctByIsomorphism;
import org.gradoop.flink.model.impl.operators.distinction.GroupByIsomorphism;
import org.gradoop.flink.model.impl.operators.equality.CollectionEquality;
import org.gradoop.flink.model.impl.operators.equality.CollectionEqualityByGraphIds;
import org.gradoop.flink.model.impl.operators.exclusion.Exclusion;
import org.gradoop.flink.model.impl.operators.intersection.Intersection;
import org.gradoop.flink.model.impl.operators.intersection.IntersectionBroadcast;
import org.gradoop.flink.model.impl.operators.limit.Limit;
import org.gradoop.flink.model.impl.operators.matching.transactional.TransactionalPatternMatching;
import org.gradoop.flink.model.impl.operators.matching.transactional.algorithm.DepthSearchMatching;
import org.gradoop.flink.model.impl.operators.matching.transactional.algorithm.PatternMatchingAlgorithm;
import org.gradoop.flink.model.impl.operators.matching.transactional.function.AddMatchesToProperties;
import org.gradoop.flink.model.impl.operators.overlap.Overlap;
import org.gradoop.flink.model.impl.operators.selection.Selection;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToIdString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToIdString;
import org.gradoop.flink.model.impl.operators.union.Union;
import org.gradoop.flink.model.impl.operators.verify.VerifyCollection;
import org.gradoop.flink.model.impl.operators.verify.VerifyGraphsContainment;

/**
 * Defines the operators that are available on a {@link BaseGraphCollection}.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> the type of the base graph
 * @param <GC> the type of the graph collection
 */
public interface BaseGraphCollectionOperators<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> {

  //----------------------------------------------------------------------------
  // Base Graph / Graph Head Getters
  //----------------------------------------------------------------------------

  /**
   * Returns base graph from collection using the given identifier. If the graph does not exist,
   * an empty base graph is returned.
   *
   * @param graphID graph identifier
   * @return base graph with given id or an empty base graph
   */
  LG getGraph(final GradoopId graphID);

  /**
   * Extracts base graphs from collection using their identifiers.
   *
   * @param identifiers graph identifiers
   * @return collection containing requested base graphs
   */
  default GC getGraphs(final GradoopId... identifiers) {
    return getGraphs(GradoopIdSet.fromExisting(identifiers));
  }
  /**
   * Extracts base graphs from collection using their identifiers.
   *
   * @param identifiers graph identifiers
   * @return collection containing requested base graphs
   */
  GC getGraphs(final GradoopIdSet identifiers);

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * Filter containing graphs based on their associated graph head.
   *
   * @param predicateFunction predicate function for graph head
   * @return collection with base graphs that fulfil the predicate
   */
  default GC select(final FilterFunction<G> predicateFunction) {
    return callForCollection(new Selection<>(predicateFunction));
  }

  /**
   * Returns the first {@code n} arbitrary logical graphs contained in that collection.
   *
   * @param n number of graphs to return from collection
   * @return subset of the graph collection
   */
  default GC limit(int n) {
    return callForCollection(new Limit<>(n));
  }

  /**
   * Matches a given pattern on a graph collection.
   * The boolean flag {@code returnEmbeddings} specifies, if the return shall be the input graphs with
   * a new property {@link AddMatchesToProperties#DEFAULT_KEY}, or a new collection consisting of the
   * constructed embeddings.
   *
   * @param query the query pattern in GDL syntax
   * @param algorithm custom pattern matching algorithm, e.g., {@link DepthSearchMatching}
   * @param returnEmbeddings if true it returns the embeddings as a new graph collection
   *                         if false it returns the input collection with a new property with key
   *                         {@link AddMatchesToProperties#DEFAULT_KEY} and value true/false if the pattern
   *                         is contained in the respective graph
   * @return a graph collection containing either the embeddings or the input
   * graphs with a new property with name {@link AddMatchesToProperties#DEFAULT_KEY}
   */
  default GC query(String query, PatternMatchingAlgorithm algorithm, boolean returnEmbeddings) {
    return callForCollection(new TransactionalPatternMatching<>(query, algorithm, returnEmbeddings));
  }

  /**
   * Verifies each graph of this collection, removing dangling edges, i.e. edges pointing to or from
   * a vertex not contained in the graph.<br>
   * This operator can be applied after an operator that has not checked these graphs validity.
   * The graph heads of these base graphs remains unchanged.
   *
   * @return this graph collection with all dangling edges removed.
   */
  default GC verify() {
    return callForCollection(new VerifyCollection<>());
  }

  /**
   * Verifies this graph collection, removing dangling graph ids from its elements,
   * i.e. ids not contained in the collection.<br>
   * This operator can be applied after an operator that has not checked the graphs validity.
   * The graph head of this graph collection remains unchanged.
   *
   * @return this graph collection with all dangling graph ids removed.
   */
  default GC verifyGraphsContainment() {
    return callForCollection(new VerifyGraphsContainment<>());
  }

  //----------------------------------------------------------------------------
  // Binary Operators
  //----------------------------------------------------------------------------

  /**
   * Returns a collection with all base graphs from two input collections.
   * Graph equality is based on their identifiers.
   *
   * @param otherCollection collection to build union with
   * @return union of both collections
   */
  default GC union(GC otherCollection) {
    return callForCollection(new Union<>(), otherCollection);
  }

  /**
   * Returns a collection with all base graphs that exist in both input
   * collections. Graph equality is based on their identifiers.
   *
   * @param otherCollection collection to build intersect with
   * @return intersection of both collections
   */
  default GC intersect(GC otherCollection) {
    return callForCollection(new Intersection<>(), otherCollection);
  }

  /**
   * Returns a collection with all base graphs that exist in both input collections.
   * Graph equality is based on their identifiers.
   * <p>
   * Implementation that works faster if {@code otherCollection} is small
   * (e.g. fits in the workers main memory).
   *
   * @param otherCollection collection to build intersect with
   * @return intersection of both collections
   */
  default GC intersectWithSmallResult(GC otherCollection) {
    return callForCollection(new IntersectionBroadcast<>(), otherCollection);
  }

  /**
   * Returns a collection with all base graphs that are contained in that
   * collection but not in the other. Graph equality is based on their identifiers.
   *
   * @param otherCollection collection to subtract from that collection
   * @return difference between that and the other collection
   */
  default GC difference(GC otherCollection) {
    return callForCollection(new Difference<>(), otherCollection);
  }

  /**
   * Returns a collection with all base graphs that are contained in that
   * collection but not in the other. Graph equality is based on their identifiers.
   * <p>
   * Alternate implementation that works faster if the intermediate result
   * (list of graph identifiers) fits into the workers memory.
   *
   * @param otherCollection collection to subtract from that collection
   * @return difference between that and the other collection
   */
  default GC differenceWithSmallResult(GC otherCollection) {
    return callForCollection(new DifferenceBroadcast<>(), otherCollection);
  }

  /**
   * Checks, if another collection contains the same graphs as this graph (by id).
   *
   * @param otherCollection other graph collection
   * @return 1-element dataset containing true, if equal by graph ids
   */
  default DataSet<Boolean> equalsByGraphIds(GC otherCollection) {
    return callForValue(new CollectionEqualityByGraphIds<>(), otherCollection);
  }

  /**
   * Checks, if another collection contains the same graphs as this graph (by vertex and edge ids).
   *
   * @param otherCollection other graph
   * @return 1-element dataset containing true, if equal by element ids
   */
  default DataSet<Boolean> equalsByGraphElementIds(GC otherCollection) {
    return callForValue(new CollectionEquality<>(
      new GraphHeadToEmptyString<>(),
      new VertexToIdString<>(),
      new EdgeToIdString<>(), true), otherCollection);
  }

  /**
   * Returns a 1-element dataset containing a {@code boolean} value which
   * indicates if the graph collection is equal to the given graph collection.
   * <p>
   * Equality is defined on the element data contained inside the collection, i.e. vertices and edges.
   * This is limited to EPGM data (IDs, Label, Properties). All extensions of EPGM are ignored.
   *
   * @param otherCollection graph collection to compare with
   * @return 1-element dataset containing {@code true} if the two collections
   * are equal or {@code false} if not
   */
  default DataSet<Boolean> equalsByGraphElementData(GC otherCollection) {
    return callForValue(new CollectionEquality<>(
      new GraphHeadToEmptyString<>(),
      new VertexToDataString<>(),
      new EdgeToDataString<>(), true), otherCollection);
  }

  /**
   * Returns a 1-element dataset containing a {@code boolean} value which
   * indicates if the graph collection is equal to the given graph collection.
   * <p>
   * Equality is defined on the data contained inside the collection, i.e. graph heads, vertices and edges.
   * This is limited to EPGM data (IDs, Label, Properties). All extensions of EPGM are ignored.
   *
   * @param otherCollection graph collection to compare with
   * @return 1-element dataset containing {@code true} if the two collections
   * are equal or {@code false} if not
   */
  default DataSet<Boolean> equalsByGraphData(GC otherCollection) {
    return callForValue(new CollectionEquality<>(
      new GraphHeadToDataString<>(),
      new VertexToDataString<>(),
      new EdgeToDataString<>(), true), otherCollection);
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  /**
   * Returns a 1-element dataset containing a {@code boolean} value which indicates if the collection
   * is empty.
   *
   * A collection is considered empty, if it contains no logical graphs.
   *
   * @return  1-element dataset containing {@code true}, if the collection is empty or {@code false} if not
   */
  DataSet<Boolean> isEmpty();

  /**
   * Returns a distinct collection of base graphs. Graph equality is based on graph identifiers.
   *
   * @return distinct graph collection
   */
  default GC distinctById() {
    return callForCollection(new DistinctById<>());
  }

  /**
   * Groups a graph collection by isomorphism.
   * Graph equality is based on isomorphism including labels and properties.
   *
   * @return distinct graph collection
   */
  default GC distinctByIsomorphism() {
    return callForCollection(new DistinctByIsomorphism<>());
  }

  /**
   * Groups a graph collection by isomorphism including labels and values.
   *
   * @param reduceFunction function to reduce all graph heads of a group into a single representative one,
   *                       e.g., to count the number of group members
   *
   * @return grouped graph collection
   */
  default GC groupByIsomorphism(GraphHeadReduceFunction<G> reduceFunction) {
    return callForCollection(new GroupByIsomorphism<>(reduceFunction));
  }

  //----------------------------------------------------------------------------
  // Call for Operators
  //----------------------------------------------------------------------------

  /**
   * Creates a value using the given unary collection to value operator.
   *
   * @param operator unary graph collection to value operator
   * @param <T> return type
   * @return result of given operator
   */
  <T> T callForValue(UnaryBaseGraphCollectionToValueOperator<GC, T> operator);

  /**
   * Calls the given binary collection to value operator using this graph collection and the
   * input graph collection.
   *
   * @param operator        binary collection to value operator
   * @param otherCollection second input collection for operator
   * @param <T> return type
   * @return result of given operator
   */
  <T> T callForValue(BinaryBaseGraphCollectionToValueOperator<GC, T> operator, GC otherCollection);

  /**
   * Calls the given unary collection to graph operator using this graph collection to get a graph of type
   * {@link LG}.
   *
   * @param operator unary collection to graph operator
   * @return result of given operator
   */
  default LG callForGraph(UnaryBaseGraphCollectionToBaseGraphOperator<GC, LG> operator) {
    return callForValue(operator);
  }

  /**
   * Creates a graph collection using the given unary graph collection operator.
   *
   * @param operator unary graph collection to graph collection operator
   * @return result of given operator
   */
  default GC callForCollection(UnaryBaseGraphCollectionToBaseGraphCollectionOperator<GC> operator) {
    return callForValue(operator);
  }

  /**
   * Calls the given binary collection to collection operator using this graph collection and the
   * input graph collection.
   *
   * @param operator        binary collection to collection operator
   * @param otherCollection second input collection for operator
   * @return result of given operator
   */
  default GC callForCollection(BinaryBaseGraphCollectionToBaseGraphCollectionOperator<GC> operator,
                               GC otherCollection) {
    return callForValue(operator, otherCollection);
  }

  /**
   * Applies a given unary graph to graph operator (e.g., aggregate) on each
   * base graph in the graph collection.
   *
   * @param operator applicable unary graph to graph operator
   * @return collection with resulting logical graphs
   */
  default GC apply(ApplicableUnaryBaseGraphToBaseGraphOperator<GC> operator) {
    return callForValue(operator);
  }

  /**
   * Transforms a graph collection into a graph by applying a
   * {@link ReducibleBinaryBaseGraphToBaseGraphOperator} pairwise on the elements of the collection.
   *
   * @param operator reducible binary graph to graph operator
   * @return base graph returned by the operator
   *
   * @see Exclusion
   * @see Overlap
   * @see Combination
   */
  default LG reduce(ReducibleBinaryBaseGraphToBaseGraphOperator<GC, LG> operator) {
    return callForValue(operator);
  }
}
