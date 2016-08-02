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

package org.gradoop.flink.model.api.operators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.model.impl.operators.exclusion.Exclusion;
import org.gradoop.flink.model.impl.operators.overlap.Overlap;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.util.Order;
import org.gradoop.flink.model.impl.GraphCollection;

/**
 * Describes all operators that can be applied on a collection of logical
 * graphs in the EPGM.
 */
public interface GraphCollectionOperators extends GraphBaseOperators {

  //----------------------------------------------------------------------------
  // Logical Graph / Graph Head Getters
  //----------------------------------------------------------------------------

  /**
   * Returns the graph heads associated with the logical graphs in that
   * collection.
   *
   * @return graph heads
   */
  DataSet<GraphHead> getGraphHeads();

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
   * Returns a distinct collection of logical graphs. Graph equality is based on
   * graph identifiers.
   *
   * @return distinct graph collection
   */
  GraphCollection distinct();

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
   * Calls the given unary collection to collection operator for the collection.
   *
   * @param op unary collection to collection operator
   * @return result of given operator
   */
  GraphCollection callForCollection(
    UnaryCollectionToCollectionOperator op);

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
   * Applies a given unary graph to graph operator (e.g., aggregate) on each
   * logical graph in the graph collection.
   *
   * @param op applicable unary graph to graph operator
   * @return collection with resulting logical graphs
   */
  GraphCollection apply(
    ApplicableUnaryGraphToGraphOperator op);

  /**
   * Transforms a graph collection into a logical graph by applying a
   * {@link BinaryGraphToGraphOperator} pairwise on the elements of the
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
   * Transforms a graph collection into a set of graph transactions.
   * @return graph transactions representing the graph collection
   */
  GraphTransactions toTransactions();
}
