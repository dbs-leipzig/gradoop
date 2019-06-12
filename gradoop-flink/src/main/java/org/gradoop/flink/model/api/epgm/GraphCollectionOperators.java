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
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.util.Order;
import org.gradoop.flink.model.api.functions.GraphHeadReduceFunction;
import org.gradoop.flink.model.api.operators.ApplicableUnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.transactional.algorithm.PatternMatchingAlgorithm;

/**
 * Defines the operators that are available on a {@link GraphCollection}.
 */
public interface GraphCollectionOperators extends GraphBaseOperators {

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
  GraphCollection select(FilterFunction<GraphHead> predicateFunction);

  /**
   * Returns a graph collection that is sorted by a given graph property key.
   *
   * @param propertyKey property which is used for comparison
   * @param order       ascending, descending
   * @return ordered collection
   */
  GraphCollection sortBy(String propertyKey, Order order);

  /**
   * Matches a given pattern on a graph collection.
   * The boolean flag specifies, if the return shall be the input graphs with
   * a new property ("contains pattern"), or a new collection consisting of the
   * constructed embeddings
   *
   * @param algorithm         custom pattern matching algorithm
   * @param pattern           query pattern
   * @param returnEmbeddings  true -> return embeddings as new collection,
   *                          false -> return collection with new property
   * @return  a graph collection containing either the embeddings or the input
   * graphs with a new property ("contains pattern")
   */
  GraphCollection match(
    String pattern,
    PatternMatchingAlgorithm algorithm,
    boolean returnEmbeddings);

  //----------------------------------------------------------------------------
  // Auxiliary operators
  //----------------------------------------------------------------------------

  /**
   * Applies a given unary graph to graph operator (e.g., aggregate) on each
   * logical graph in the graph collection.
   *
   * @param op applicable unary graph to graph operator
   * @return collection with resulting logical graphs
   */
  GraphCollection apply(
    ApplicableUnaryGraphToGraphOperator op);

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
}
