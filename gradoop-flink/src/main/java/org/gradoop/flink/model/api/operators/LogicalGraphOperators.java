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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.TraverserStrategy;

import java.util.List;

/**
 * Describes all operators that can be applied on a single logical graph in the
 * EPGM.
 */
public interface LogicalGraphOperators extends GraphBaseOperators {

  /**
   * Returns a dataset containing a single graph head associated with that
   * logical graph.
   *
   * @return 1-element dataset
   */
  DataSet<GraphHead> getGraphHead();

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism
   * strategies, which is vertex homomorphism and edge isomorphism. The vertex and edge data of
   * the data graph elements is attached to the resulting vertices.
   *
   * @param query Cypher query
   * @param graphStatistics statistics about the data graph
   * @return graph collection containing matching subgraphs
   */
  GraphCollection cypher(String query, GraphStatistics graphStatistics);

  /**
   * Evaluates the given query using the Cypher query engine.
   *
   * @param query Cypher query
   * @param attachData  attach original vertex and edge data to the result
   * @param vertexStrategy morphism setting for vertex mapping
   * @param edgeStrategy morphism setting for edge mapping
   * @param graphStatistics statistics about the data graph
   * @return graph collection containing matching subgraphs
   */
  GraphCollection cypher(String query, boolean attachData,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy, GraphStatistics graphStatistics);

  /**
   * Evaluates the given GDL query using the Traverser query engine.
   *
   * @param pattern  GDL graph pattern
   *
   * @return subgraphs of the input graph that match the given graph pattern
   */
  GraphCollection match(String pattern);

  /**
   * Evaluates the given GDL query using the Traverser query engine.
   *
   * This method allows to control if the original vertex and edge data
   * (labels and properties) shall be attached to the resulting subgraphs.
   * Note that this requires additional JOIN operations.
   *
   * @param pattern     GDL graph pattern
   * @param attachData  attach original vertex and edge data to the result
   * @return subgraphs of the input graph that match the given graph pattern
   */
  GraphCollection match(String pattern, boolean attachData);

  /**
   * Evaluates the given GDL query using the Traverser query engine.
   *
   * This method allows to control the match strategy. This influences mostly
   * if vertices and edges can be matched to multiple vertices/edges in the
   * query.
   *
   * @param pattern           GDL graph pattern
   * @param attachData        attach original vertex and edge data to the result
   * @param matchStrategy     strategy for vertex and edge mappings
   * @param iterationStrategy strategy for internal iteration
   * @return subgraphs of the input graph that match the given graph pattern
   */
  GraphCollection match(String pattern, boolean attachData,
    MatchStrategy matchStrategy, TraverserStrategy iterationStrategy);

  /**
   * Creates a copy of the logical graph.
   *
   * Note that this method creates new graph head, vertex and edge instances.
   *
   * @return projected logical graph
   */
  LogicalGraph copy();

  /**
   * Transforms the elements of the logical graph using the given transformation
   * functions. The identity of the elements is preserved.
   *
   * @param graphHeadTransformationFunction graph head transformation function
   * @param vertexTransformationFunction    vertex transformation function
   * @param edgeTransformationFunction      edge transformation function
   * @return transformed logical graph
   */
  LogicalGraph transform(
    TransformationFunction<GraphHead> graphHeadTransformationFunction,
    TransformationFunction<Vertex> vertexTransformationFunction,
    TransformationFunction<Edge> edgeTransformationFunction);

  /**
   * Transforms the graph head of the logical graph using the given
   * transformation function. The identity of the graph is preserved.
   *
   * @param graphHeadTransformationFunction graph head transformation function
   * @return transformed logical graph
   */
  LogicalGraph transformGraphHead(
    TransformationFunction<GraphHead> graphHeadTransformationFunction);

  /**
   * Transforms the vertices of the logical graph using the given transformation
   * function. The identity of the vertices is preserved.
   *
   * @param vertexTransformationFunction vertex transformation function
   * @return transformed logical graph
   */
  LogicalGraph transformVertices(
    TransformationFunction<Vertex> vertexTransformationFunction);

  /**
   * Transforms the edges of the logical graph using the given transformation
   * function. The identity of the edges is preserved.
   *
   * @param edgeTransformationFunction edge transformation function
   * @return transformed logical graph
   */
  LogicalGraph transformEdges(
    TransformationFunction<Edge> edgeTransformationFunction);

  /**
   * Returns the subgraph that is induced by the vertices which fulfill the
   * given filter function.
   *
   * @param vertexFilterFunction vertex filter function
   * @return vertex-induced subgraph as a new logical graph
   */
  LogicalGraph vertexInducedSubgraph(
    FilterFunction<Vertex> vertexFilterFunction);

  /**
   * Returns the subgraph that is induced by the edges which fulfill the given
   * filter function.
   *
   * @param edgeFilterFunction edge filter function
   * @return edge-induced subgraph as a new logical graph
   */
  LogicalGraph edgeInducedSubgraph(
    FilterFunction<Edge> edgeFilterFunction);

  /**
   * Returns a subgraph of the logical graph which contains only those vertices
   * and edges that fulfil the given vertex and edge filter function
   * respectively.
   *
   * Note, that the operator does not verify the consistency of the resulting
   * graph. Use {#toGellyGraph().subgraph()} for that behaviour.
   *
   * @param vertexFilterFunction  vertex filter function
   * @param edgeFilterFunction    edge filter function
   * @return  logical graph which fulfils the given predicates and is a subgraph
   *          of that graph
   */
  LogicalGraph subgraph(FilterFunction<Vertex> vertexFilterFunction,
    FilterFunction<Edge> edgeFilterFunction);

  /**
   * Applies the given aggregate function to the logical graph and stores the
   * result of that function at the resulting graph using the given property
   * key.
   *
   * @param aggregateFunc computes an aggregate on the logical graph
   * @return logical graph with additional property storing the aggregate
   */
  LogicalGraph aggregate(AggregateFunction aggregateFunc);

  /**
   * Creates a new graph from a randomly chosen subset of nodes and their
   * associated edges.
   *
   * @param sampleSize relative amount of nodes in the result graph
   * @return logical graph with random nodes and their associated edges
   */
  LogicalGraph sampleRandomNodes(float sampleSize);

  /**
   * Creates a condensed version of the logical graph by grouping vertices based on the specified
   * property keys.
   *
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices.
   *
   * Note: To group vertices by their type label, one needs to add the specific symbol
   * {@link Grouping#LABEL_SYMBOL} to the respective grouping keys.
   *
   * @param vertexGroupingKeys property keys to group vertices
   *
   * @return summary graph
   * @see Grouping
   */
  LogicalGraph groupBy(List<String> vertexGroupingKeys);

  /**
   * Creates a condensed version of the logical graph by grouping vertices and edges based on given
   * property keys.
   *
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices and explicitly by the specified edge grouping keys.
   *
   * One needs to at least specify a list of vertex grouping keys. Any other argument may be
   * {@code null}.
   *
   * Note: To group vertices/edges by their type label, one needs to add the specific symbol
   * {@link Grouping#LABEL_SYMBOL} to the respective grouping keys.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @param edgeGroupingKeys property keys to group edges
   *
   * @return summary graph
   * @see Grouping
   */
  LogicalGraph groupBy(List<String> vertexGroupingKeys, List<String> edgeGroupingKeys);

  /**
   * Creates a condensed version of the logical graph by grouping vertices and edges based on given
   * property keys.
   *
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices and explicitly by the specified edge grouping keys. Furthermore, one can
   * specify sets of vertex and edge aggregate functions which are applied on vertices/edges
   * represented by the same super vertex/edge.
   *
   * One needs to at least specify a list of vertex grouping keys. Any other argument may be
   * {@code null}.
   *
   * Note: To group vertices/edges by their type label, one needs to add the specific symbol
   * {@link Grouping#LABEL_SYMBOL} to the respective grouping keys.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @param vertexAggregateFunctions aggregate functions to apply on super vertices
   * @param edgeGroupingKeys property keys to group edges
   * @param edgeAggregateFunctions aggregate functions to apply on super edges
   * @param groupingStrategy execution strategy for vertex grouping
   *
   * @return summary graph
   * @see Grouping
   */
  LogicalGraph groupBy(
    List<String> vertexGroupingKeys, List<PropertyValueAggregator> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<PropertyValueAggregator> edgeAggregateFunctions,
    GroupingStrategy groupingStrategy);

  /**
   * Checks, if another logical graph contains exactly the same vertices and
   * edges (by id) as this graph.
   *
   * @param other other graph
   * @return 1-element dataset containing true, if equal by element ids
   */
  DataSet<Boolean> equalsByElementIds(LogicalGraph other);

  /**
   * Checks, if another logical graph contains vertices and edges with the same
   * attached data (i.e. label and properties) as this graph.
   *
   * @param other other graph
   * @return 1-element dataset containing true, iff equal by element data
   */
  DataSet<Boolean> equalsByElementData(LogicalGraph other);

  /**
   * Checks, if another logical graph has the same attached data and contains
   * vertices and edges with the same attached data as this graph.
   *
   * @param other other graph
   * @return 1-element dataset containing true, iff equal by element data
   */
  DataSet<Boolean> equalsByData(LogicalGraph other);

  //----------------------------------------------------------------------------
  // Binary Operators
  //----------------------------------------------------------------------------

  /**
   * Creates a new logical graph by combining the vertex and edge sets of
   * this graph and the given graph. Vertex and edge equality is based on their
   * identifiers.
   *
   * @param otherGraph logical graph to combine this graph with
   * @return logical graph containing all vertices and edges of the
   * input graphs
   */
  LogicalGraph combine(LogicalGraph otherGraph);

  /**
   * Creates a new logical graph containing the overlapping vertex and edge
   * sets of this graph and the given graph. Vertex and edge equality is
   * based on their identifiers.
   *
   * @param otherGraph logical graph to compute overlap with
   * @return logical graph that contains all vertices and edges that exist in
   * both input graphs
   */
  LogicalGraph overlap(LogicalGraph otherGraph);

  /**
   * Creates a new logical graph containing only vertices and edges that
   * exist in that graph but not in the other graph. Vertex and edge equality
   * is based on their identifiers.
   *
   * @param otherGraph logical graph to exclude from that graph
   * @return logical that contains only vertices and edges that are not in
   * the other graph
   */
  LogicalGraph exclude(LogicalGraph otherGraph);

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  /**
   * Splits the graph into multiple logical graphs using the property value
   * which is assigned to the given property key. Vertices and edges that do
   * not have this property will be removed from the resulting collection.
   *
   * @param propertyKey split property key
   * @return graph collection
   */
  GraphCollection splitBy(String propertyKey);

  /**
   * Creates a logical graph using the given unary graph operator.
   *
   * @param operator unary graph to graph operator
   * @return result of given operator
   */
  LogicalGraph callForGraph(UnaryGraphToGraphOperator operator);

  /**
   * Creates a logical graph from that graph and the input graph using the
   * given binary operator.
   *
   * @param operator   binary graph to graph operator
   * @param otherGraph other graph
   * @return result of given operator
   */
  LogicalGraph callForGraph(BinaryGraphToGraphOperator operator, LogicalGraph otherGraph);

  /**
   * Creates a graph collection from that grpah using the given unary graph
   * operator.
   *
   * @param operator unary graph to collection operator
   * @return result of given operator
   */
  GraphCollection callForCollection(UnaryGraphToCollectionOperator operator);
}
