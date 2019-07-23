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
package org.gradoop.flink.model.api.epgm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.util.Order;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.api.functions.GraphHeadReduceFunction;
import org.gradoop.flink.model.api.operators.BinaryCollectionToCollectionOperator;
import org.gradoop.flink.model.api.operators.ReducibleBinaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryCollectionToGraphOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.model.impl.operators.exclusion.Exclusion;
import org.gradoop.flink.model.impl.operators.matching.transactional.algorithm.DepthSearchMatching;
import org.gradoop.flink.model.impl.operators.matching.transactional.algorithm.PatternMatchingAlgorithm;
import org.gradoop.flink.model.impl.operators.matching.transactional.function.AddMatchesToProperties;
import org.gradoop.flink.model.impl.operators.overlap.Overlap;

import java.io.IOException;

/**
 * Defines the operators that are available on a {@link GraphCollection}.
 */
public interface GraphCollectionOperators {

  //----------------------------------------------------------------------------
  // Logical Graph / Graph Head Getters
  //----------------------------------------------------------------------------

  /**
   * Returns logical graph from collection using the given identifier. If the
   * graph does not exist, an empty logical graph is returned.
   *
   * @param graphID graph identifier
   * @return logical graph with given id or an empty logical graph
   */
  LogicalGraph getGraph(final GradoopId graphID);
  /**
   * Extracts logical graphs from collection using their identifiers.
   *
   * @param identifiers graph identifiers
   * @return collection containing requested logical graphs
   */
  GraphCollection getGraphs(final GradoopId... identifiers);

  /**
   * Extracts logical graphs from collection using their identifiers.
   *
   * @param identifiers graph identifiers
   * @return collection containing requested logical graphs
   */
  GraphCollection getGraphs(GradoopIdSet identifiers);

  //----------------------------------------------------------------------------
  // Unary operators
  //----------------------------------------------------------------------------

  /**
   * Filter containing graphs based on their associated graph head.
   *
   * @param predicateFunction predicate function for graph head
   * @return collection with logical graphs that fulfil the predicate
   */
  GraphCollection select(FilterFunction<EPGMGraphHead> predicateFunction);

  /**
   * Returns a graph collection that is sorted by a given graph property key.
   *
   * @param propertyKey property which is used for comparison
   * @param order       ascending, descending
   * @return ordered collection
   */
  GraphCollection sortBy(String propertyKey, Order order);

  /**
   * Returns the first {@code n} arbitrary logical graphs contained in that
   * collection.
   *
   * @param n number of graphs to return from collection
   * @return subset of the graph collection
   */
  GraphCollection limit(int n);

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
  GraphCollection query(String query, PatternMatchingAlgorithm algorithm, boolean returnEmbeddings);

  //----------------------------------------------------------------------------
  // Binary operators
  //----------------------------------------------------------------------------

  /**
   * Returns a collection with all logical graphs from two input collections.
   * Graph equality is based on their identifiers.
   *
   * @param otherCollection collection to build union with
   * @return union of both collections
   */
  GraphCollection union(GraphCollection otherCollection);

  /**
   * Returns a collection with all logical graphs that exist in both input
   * collections. Graph equality is based on their identifiers.
   *
   * @param otherCollection collection to build intersect with
   * @return intersection of both collections
   */
  GraphCollection intersect(GraphCollection otherCollection);

  /**
   * Returns a collection with all logical graphs that exist in both input
   * collections. Graph equality is based on their identifiers.
   * <p>
   * Implementation that works faster if {@code otherCollection} is small
   * (e.g. fits in the workers main memory).
   *
   * @param otherCollection collection to build intersect with
   * @return intersection of both collections
   */
  GraphCollection intersectWithSmallResult(
    GraphCollection otherCollection);

  /**
   * Returns a collection with all logical graphs that are contained in that
   * collection but not in the other. Graph equality is based on their
   * identifiers.
   *
   * @param otherCollection collection to subtract from that collection
   * @return difference between that and the other collection
   */
  GraphCollection difference(GraphCollection otherCollection);

  /**
   * Returns a collection with all logical graphs that are contained in that
   * collection but not in the other. Graph equality is based on their
   * identifiers.
   * <p>
   * Alternate implementation that works faster if the intermediate result
   * (list of graph identifiers) fits into the workers memory.
   *
   * @param otherCollection collection to subtract from that collection
   * @return difference between that and the other collection
   */
  GraphCollection differenceWithSmallResult(
    GraphCollection otherCollection);

  /**
   * Checks, if another collection contains the same graphs as this graph
   * (by id).
   *
   * @param other other graph
   * @return 1-element dataset containing true, if equal by graph ids
   */
  DataSet<Boolean> equalsByGraphIds(GraphCollection other);

  /**
   * Checks, if another collection contains the same graphs as this graph
   * (by vertex and edge ids).
   *
   * @param other other graph
   * @return 1-element dataset containing true, if equal by element ids
   */
  DataSet<Boolean> equalsByGraphElementIds(GraphCollection other);

  /**
   * Returns a 1-element dataset containing a {@code boolean} value which
   * indicates if the graph collection is equal to the given graph collection.
   *
   * Equality is defined on the element data contained inside the collection,
   * i.e. vertices and edges.
   *
   * @param other graph collection to compare with
   * @return  1-element dataset containing {@code true} if the two collections
   *          are equal or {@code false} if not
   */
  DataSet<Boolean> equalsByGraphElementData(GraphCollection other);

  /**
   * Returns a 1-element dataset containing a {@code boolean} value which
   * indicates if the graph collection is equal to the given graph collection.
   *
   * Equality is defined on the data contained inside the collection, i.e.
   * graph heads, vertices and edges.
   *
   * @param other graph collection to compare with
   * @return  1-element dataset containing {@code true} if the two collections
   *          are equal or {@code false} if not
   */
  DataSet<Boolean> equalsByGraphData(GraphCollection other);

  //----------------------------------------------------------------------------
  // Auxiliary operators
  //----------------------------------------------------------------------------

  /**
   * Calls the given binary collection to collection operator using that
   * graph and the input graph.
   *
   * @param op              binary collection to collection operator
   * @param otherCollection second input collection for operator
   * @return result of given operator
   */
  GraphCollection callForCollection(
    BinaryCollectionToCollectionOperator op,
    GraphCollection otherCollection);

  /**
   * Calls the given unary collection to graph operator for the collection.
   *
   * @param op unary collection to graph operator
   * @return result of given operator
   */
  LogicalGraph callForGraph(
    UnaryCollectionToGraphOperator op);

  /**
   * Transforms a graph collection into a logical graph by applying a
   * {@link ReducibleBinaryGraphToGraphOperator} pairwise on the elements of the
   * collection.
   *
   * @param op reducible binary graph to graph operator
   * @return logical graph
   *
   * @see Exclusion
   * @see Overlap
   * @see Combination
   */
  LogicalGraph reduce(ReducibleBinaryGraphToGraphOperator op);

  /**
   * Returns a distinct collection of logical graphs.
   * Graph equality is based on graph identifiers.
   *
   * @return distinct graph collection
   */
  GraphCollection distinctById();

  /**
   * Groups a graph collection by isomorphism.
   * Graph equality is based on isomorphism including labels and properties.
   *
   * @return distinct graph collection
   */
  GraphCollection distinctByIsomorphism();

  /**
   * Groups a graph collection by isomorphism including labels and values.
   *
   * @param func function to reduce all graph heads of a group into a single representative one,
   *             e.g., to count the number of group members
   *
   * @return grouped graph collection
   */
  GraphCollection groupByIsomorphism(GraphHeadReduceFunction func);

  /**
   * Returns a 1-element dataset containing a {@code boolean} value which
   * indicates if the collection is empty.
   *
   * A collection is considered empty, if it contains no logical graphs.
   *
   * @return  1-element dataset containing {@code true}, if the collection is
   *          empty or {@code false} if not
   */
  DataSet<Boolean> isEmpty();

  /**
     * Writes the graph collection to the given data sink.
     *
     * @param dataSink The data sink to which the graph collection should be written.
     * @throws IOException if the collection can't be written to the sink
     */
  void writeTo(DataSink dataSink) throws IOException;

  /**
     * Writes the graph collection to the given data sink with an optional overwrite option.
     *
     * @param dataSink The data sink to which the graph collection should be written.
     * @param overWrite determines whether existing files are overwritten
     * @throws IOException if the collection can't be written to the sink
     */
  void writeTo(DataSink dataSink, boolean overWrite) throws IOException;
}
