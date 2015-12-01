/*
 * This file is part of Gradoop.
 *
 *     Gradoop is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     Foobar is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.model.api.operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.util.Order;
import org.gradoop.model.api.functions.Predicate;
import org.gradoop.model.impl.GraphCollection;

/**
 * Describes all operators that can be applied on a collection of logical
 * graphs in the EPGM.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public interface GraphCollectionOperators
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {

  /**
   * Returns logical graph from collection using the given identifier.
   *
   * @param graphID graph identifier
   * @return logical graph with given id or {@code null} if not contained
   * @throws Exception
   */
  LogicalGraph<G, V, E> getGraph(final GradoopId graphID) throws Exception;

  /**
   * Extracts logical graphs from collection using their identifiers.
   *
   * @param identifiers graph identifiers
   * @return collection containing requested logical graphs
   * @throws Exception
   */
  GraphCollection<G, V, E> getGraphs(final GradoopId... identifiers) throws
    Exception;

  /**
   * Extracts logical graphs from collection using their identifiers.
   *
   * @param identifiers graph identifiers
   * @return collection containing requested logical graphs
   * @throws Exception
   */
  GraphCollection<G, V, E> getGraphs(GradoopIdSet identifiers) throws
    Exception;

  /**
   * Returns the number of logical graphs contained in that collection.
   *
   * @return number of logical graphs
   * @throws Exception
   */
  long getGraphCount() throws Exception;

  /**
   * Filter containing graphs based on their associated graph data.
   *
   * @param predicateFunction predicate function for graph data
   * @return collection with logical graphs that fulfil the predicate
   * @throws Exception
   */
  GraphCollection<G, V, E> filter(Predicate<G> predicateFunction) throws
    Exception;

  /**
   * Returns a collection with logical graphs that fulfil the given predicate
   * function.
   *
   * @param predicateFunction predicate function
   * @return logical graphs that fulfil the predicate
   * @throws Exception
   */
  GraphCollection<G, V, E> select(
    Predicate<LogicalGraph<G, V, E>> predicateFunction) throws Exception;

  /**
   * Returns a collection with all logical graphs from two input collections.
   * Graph equality is based on their identifiers.
   *
   * @param otherCollection collection to build union with
   * @return union of both collections
   * @throws Exception
   */
  GraphCollection<G, V, E> union(
    GraphCollection<G, V, E> otherCollection) throws Exception;

  /**
   * Returns a collection with all logical graphs that exist in both input
   * collections. Graph equality is based on their identifiers.
   *
   * @param otherCollection collection to build intersect with
   * @return intersection of both collections
   * @throws Exception
   */
  GraphCollection<G, V, E> intersect(
    GraphCollection<G, V, E> otherCollection) throws Exception;

  /**
   * Returns a collection with all logical graphs that exist in both input
   * collections. Graph equality is based on their identifiers.
   * <p>
   * Implementation that works faster if {@code otherCollection} is small
   * (e.g. fits in the workers main memory).
   *
   * @param otherCollection collection to build intersect with
   * @return intersection of both collections
   * @throws Exception
   */
  GraphCollection<G, V, E> intersectWithSmallResult(
    GraphCollection<G, V, E> otherCollection) throws Exception;

  /**
   * Returns a collection with all logical graphs that are contained in that
   * collection but not in the other. Graph equality is based on their
   * identifiers.
   *
   * @param otherCollection collection to subtract from that collection
   * @return difference between that and the other collection
   * @throws Exception
   */
  GraphCollection<G, V, E> difference(
    GraphCollection<G, V, E> otherCollection) throws Exception;

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
   * @throws Exception
   */
  GraphCollection<G, V, E> differenceWithSmallResult(
    GraphCollection<G, V, E> otherCollection) throws Exception;

  /**
   * Returns a distinct collection of logical graphs. Graph equality is based on
   * graph identifiers.
   *
   * @return distinct graph collection
   */
  GraphCollection<G, V, E> distinct();

  /**
   * Returns a graph collection that is sorted by a given graph property key.
   *
   * @param propertyKey property which is used for comparison
   * @param order       ascending, descending
   * @return ordered collection
   */
  GraphCollection<G, V, E> sortBy(String propertyKey, Order order);

  /**
   * Returns the first {@code limit} logical graphs contained in that
   * collection.
   *
   * @param limit number of graphs to return from collection
   * @return part of graph collection
   */
  GraphCollection<G, V, E> top(int limit);

  /**
   * Applies a given unary graph to graph operator (e.g., summarize) on each
   * logical graph in the graph collection.
   *
   * @param op unary graph to graph operator
   * @return collection with resulting logical graphs
   */
  GraphCollection<G, V, E> apply(UnaryGraphToGraphOperator<G, V, E> op);

  /**
   * Applies binary graph to graph operator (e.g., combine) on each pair of
   * logical graphs in that collection and produces a single output graph.
   *
   * @param op binary graph to graph operator
   * @return logical graph
   */
  LogicalGraph<G, V, E> reduce(BinaryGraphToGraphOperator<G, V, E> op);

  /**
   * Calls the given unary collection to collection operator for the collection.
   *
   * @param op unary collection to collection operator
   * @return result of given operator
   */
  GraphCollection<G, V, E> callForCollection(
    UnaryCollectionToCollectionOperator<V, E, G> op);

  /**
   * Calls the given binary collection to collection operator using that
   * graph and the input graph.
   *
   * @param op              binary collection to collection operator
   * @param otherCollection second input collection for operator
   * @return result of given operator
   * @throws Exception
   */
  GraphCollection<G, V, E> callForCollection(
    BinaryCollectionToCollectionOperator<G, V, E> op,
    GraphCollection<G, V, E> otherCollection) throws Exception;

  /**
   * Calls the given unary collection to graph operator for the collection.
   *
   * @param op unary collection to graph operator
   * @return result of given operator
   */
  LogicalGraph<G, V, E> callForGraph(
    UnaryCollectionToGraphOperator<G, V, E> op);

  /**
   * Writes the graph collection into three separate JSON files. {@code
   * vertexFile} contains the vertex data of all logical graphs, {@code
   * edgeFile} contains the edge data of all logical graphs and {@code
   * graphFile} contains the graph data the logical graphs in the collection.
   * <p>
   * Operation uses Flink to write the internal datasets, thus writing to
   * local file system ({@code file://}) as well as HDFS ({@code hdfs://}) is
   * supported.
   *
   * @param vertexFile vertex data output file
   * @param edgeFile   edge data output file
   * @param graphFile  graph data output file
   * @throws Exception
   */
  void writeAsJson(final String vertexFile, final String edgeFile,
    final String graphFile) throws Exception;

  /**
   * Checks, if another collection contains the same graphs as this graph
   * (by id).
   *
   * @param other other graph
   * @return 1-element dataset containing true, if equal by graph ids
   */
  DataSet<Boolean> equalsByGraphIds(GraphCollection<G, V, E> other);

  /**
   * Convenience method for collected result of
   * {@link #equalsByGraphIds(GraphCollection)}
   *
   * @param other other graph
   * @return true, if equal by graph ids
   * @throws Exception
   */
  Boolean equalsByGraphIdsCollected(GraphCollection<G, V, E> other) throws
    Exception;

  /**
   * Checks, if another collection contains the same graphs as this graph
   * (by vertex and edge ids).
   *
   * @param other other graph
   * @return 1-element dataset containing true, if equal by element ids
   */
  DataSet<Boolean> equalsByGraphElementIds(GraphCollection<G, V, E> other);

  /**
   * Convenience method for collected result of
   * {@link #equalsByGraphElementIds(GraphCollection)}
   *
   * @param other other graph
   * @return true, if equal by element ids
   * @throws Exception
   */
  Boolean equalsByGraphElementIdsCollected(
    GraphCollection<G, V, E> other) throws Exception;
}
