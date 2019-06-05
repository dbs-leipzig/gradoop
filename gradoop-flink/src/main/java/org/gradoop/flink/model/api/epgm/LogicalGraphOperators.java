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
import org.gradoop.common.model.impl.metadata.MetaData;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.GraphsToGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.cypher.capf.result.CAPFQueryResult;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.neighborhood.Neighborhood;
import org.gradoop.flink.model.impl.operators.sampling.SamplingAlgorithm;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;

import java.util.List;
import java.util.Objects;

/**
 * Defines the operators that are available on a {@link LogicalGraph}.
 */
public interface LogicalGraphOperators extends GraphBaseOperators {

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * Evaluates the given cypher query using CAPF (Cypher for Apache Flink). CAPF implements the
   * default cypher morphism strategies, which is vertex homomorphism and edge isomorphism. The
   * result is a CAPFQueryResult, containing a flink table that can be converted to a
   * GraphCollection, if it contains vertices or edges.
   *
   * @param query    the query string
   * @param metaData metaData object
   * @return the result, containing a flink table and possibly a GraphCollection
   */
  CAPFQueryResult cypher(String query, MetaData metaData) throws Exception;

  /**
   * Evaluates the given cypher query using CAPF (Cypher for Apache Flink). CAPF implements the
   * default cypher morphism strategies, which is vertex homomorphism and edge isomorphism. The
   * result is a CAPFQueryResult, containing a flink table that can be converted to a
   * GraphCollection, if it contains vertices or edges.
   * <p>
   * In this overloaded function, the property maps are constructed automatically. This is
   * a lot slower and actually requires the job to be split in two parts to collect the property
   * maps.
   *
   * @param query the query string
   * @return the result, containing a flink table and possibly a GraphCollection
   */
  CAPFQueryResult cypher(String query) throws Exception;

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism
   * strategies, which is vertex homomorphism and edge isomorphism. The vertex and edge data of
   * the data graph elements is attached to the resulting vertices.
   * <p>
   * Note, that this method used no statistics about the data graph which may result in bad
   * runtime performance. Use {@link LogicalGraphOperators#query(String, GraphStatistics)} to
   * provide statistics for the query planner.
   *
   * @param query Cypher query
   * @return graph collection containing matching subgraphs
   */
  GraphCollection query(String query);

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism
   * strategies, which is vertex homomorphism and edge isomorphism. The vertex and edge data of
   * the data graph elements is attached to the resulting vertices.
   * <p>
   * Note, that this method used no statistics about the data graph which may result in bad
   * runtime performance. Use {@link LogicalGraphOperators#query(String, GraphStatistics)} to
   * provide statistics for the query planner.
   * <p>
   * In addition, the operator can be supplied with a construction pattern allowing the creation
   * of new graph elements based on variable bindings of the match pattern. Consider the following
   * example:
   *
   * <pre>
   * <code>graph.query(
   *  "MATCH (a:Author)-[:WROTE]->(:Paper)<-[:WROTE]-(b:Author) WHERE a <> b",
   *  "(a)-[:CO_AUTHOR]->(b)")
   * </code>
   * </pre>
   * <p>
   * The query pattern is looking for pairs of authors that worked on the same paper. The
   * construction pattern defines a new edge of type CO_AUTHOR between the two entities.
   *
   * @param query               Cypher query string
   * @param constructionPattern Construction pattern
   * @return graph collection containing the output of the construct pattern
   */
  GraphCollection query(String query, String constructionPattern);

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism
   * strategies, which is vertex homomorphism and edge isomorphism. The vertex and edge data of
   * the data graph elements is attached to the resulting vertices.
   *
   * @param query           Cypher query
   * @param graphStatistics statistics about the data graph
   * @return graph collection containing matching subgraphs
   */
  GraphCollection query(String query, GraphStatistics graphStatistics);

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism
   * strategies, which is vertex homomorphism and edge isomorphism. The vertex and edge data of
   * the data graph elements is attached to the resulting vertices.
   * <p>
   * In addition, the operator can be supplied with a construction pattern allowing the creation
   * of new graph elements based on variable bindings of the match pattern. Consider the following
   * example:
   *
   * <pre>
   * <code>graph.query(
   *  "MATCH (a:Author)-[:WROTE]->(:Paper)<-[:WROTE]-(b:Author) WHERE a <> b",
   *  "(a)-[:CO_AUTHOR]->(b)")
   * </code>
   * </pre>
   * <p>
   * The query pattern is looking for pairs of authors that worked on the same paper. The
   * construction pattern defines a new edge of type CO_AUTHOR between the two entities.
   *
   * @param query               Cypher query
   * @param constructionPattern Construction pattern
   * @param graphStatistics     statistics about the data graph
   * @return graph collection containing the output of the construct pattern
   */
  GraphCollection query(String query, String constructionPattern, GraphStatistics graphStatistics);

  /**
   * Evaluates the given query using the Cypher query engine.
   *
   * @param query           Cypher query
   * @param attachData      attach original vertex and edge data to the result
   * @param vertexStrategy  morphism setting for vertex mapping
   * @param edgeStrategy    morphism setting for edge mapping
   * @param graphStatistics statistics about the data graph
   * @return graph collection containing matching subgraphs
   */
  GraphCollection query(String query, boolean attachData, MatchStrategy vertexStrategy,
    MatchStrategy edgeStrategy, GraphStatistics graphStatistics);

  /**
   * Evaluates the given query using the Cypher query engine.
   *
   * @param query               Cypher query
   * @param constructionPattern Construction pattern
   * @param attachData          attach original vertex and edge data to the result
   * @param vertexStrategy      morphism setting for vertex mapping
   * @param edgeStrategy        morphism setting for edge mapping
   * @param graphStatistics     statistics about the data graph
   * @return graph collection containing matching subgraphs
   */
  GraphCollection query(String query, String constructionPattern, boolean attachData,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy,
    GraphStatistics graphStatistics);

  /**
   * Creates a copy of the logical graph.
   * <p>
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
  LogicalGraph transformVertices(TransformationFunction<Vertex> vertexTransformationFunction);

  /**
   * Transforms the edges of the logical graph using the given transformation
   * function. The identity of the edges is preserved.
   *
   * @param edgeTransformationFunction edge transformation function
   * @return transformed logical graph
   */
  LogicalGraph transformEdges(TransformationFunction<Edge> edgeTransformationFunction);

  /**
   * Returns the subgraph that is induced by the vertices which fulfill the
   * given filter function.
   *
   * @param vertexFilterFunction vertex filter function
   * @return vertex-induced subgraph as a new logical graph
   */
  LogicalGraph vertexInducedSubgraph(FilterFunction<Vertex> vertexFilterFunction);

  /**
   * Returns the subgraph that is induced by the edges which fulfill the given
   * filter function.
   *
   * @param edgeFilterFunction edge filter function
   * @return edge-induced subgraph as a new logical graph
   */
  LogicalGraph edgeInducedSubgraph(FilterFunction<Edge> edgeFilterFunction);

  /**
   * Returns a subgraph of the logical graph which contains only those vertices
   * and edges that fulfil the given vertex and edge filter function
   * respectively.
   * <p>
   * Note, that the operator does not verify the consistency of the resulting
   * graph. Use {#toGellyGraph().subgraph()} for that behaviour.
   *
   * @param vertexFilterFunction vertex filter function
   * @param edgeFilterFunction   edge filter function
   * @return logical graph which fulfils the given predicates and is a subgraph
   * of that graph
   */
  default LogicalGraph subgraph(FilterFunction<Vertex> vertexFilterFunction,
    FilterFunction<Edge> edgeFilterFunction) {
    Objects.requireNonNull(vertexFilterFunction);
    Objects.requireNonNull(edgeFilterFunction);
    return subgraph(vertexFilterFunction, edgeFilterFunction, Subgraph.Strategy.BOTH);
  }

  /**
   * Returns a subgraph of the logical graph which contains only those vertices
   * and edges that fulfil the given vertex and edge filter function
   * respectively.
   * <p>
   * Note, that the operator does not verify the consistency of the resulting
   * graph. Use {#toGellyGraph().subgraph()} for that behaviour.
   *
   * @param vertexFilterFunction vertex filter function
   * @param edgeFilterFunction   edge filter function
   * @param strategy             execution strategy for the operator
   * @return logical graph which fulfils the given predicates and is a subgraph
   * of that graph
   */
  LogicalGraph subgraph(FilterFunction<Vertex> vertexFilterFunction,
    FilterFunction<Edge> edgeFilterFunction, Subgraph.Strategy strategy);

  /**
   * Applies the given aggregate functions to the logical graph and stores the
   * result of those functions at the resulting graph using the given property
   * keys.
   *
   * @param aggregateFunctions computes aggregates on the logical graph
   * @return logical graph with additional properties storing the aggregates
   */
  LogicalGraph aggregate(AggregateFunction... aggregateFunctions);

  /**
   * Creates a new graph from a randomly chosen subset of nodes and their
   * associated edges.
   *
   * @param algorithm used sampling algorithm
   * @return logical graph with random nodes and their associated edges
   */
  LogicalGraph sample(SamplingAlgorithm algorithm);

  /**
   * Creates a condensed version of the logical graph by grouping vertices based on the specified
   * property keys.
   * <p>
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices.
   * <p>
   * Note: To group vertices by their type label, one needs to add the specific symbol
   * {@link Grouping#LABEL_SYMBOL} to the respective grouping keys.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @return summary graph
   * @see Grouping
   */
  LogicalGraph groupBy(List<String> vertexGroupingKeys);

  /**
   * Creates a condensed version of the logical graph by grouping vertices and edges based on given
   * property keys.
   * <p>
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices and explicitly by the specified edge grouping keys.
   * <p>
   * One needs to at least specify a list of vertex grouping keys. Any other argument may be
   * {@code null}.
   * <p>
   * Note: To group vertices/edges by their type label, one needs to add the specific symbol
   * {@link Grouping#LABEL_SYMBOL} to the respective grouping keys.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @param edgeGroupingKeys   property keys to group edges
   * @return summary graph
   * @see Grouping
   */
  LogicalGraph groupBy(List<String> vertexGroupingKeys, List<String> edgeGroupingKeys);

  /**
   * Creates a condensed version of the logical graph by grouping vertices and edges based on given
   * property keys.
   * <p>
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices and explicitly by the specified edge grouping keys. Furthermore, one can
   * specify sets of vertex and edge aggregate functions which are applied on vertices/edges
   * represented by the same super vertex/edge.
   * <p>
   * One needs to at least specify a list of vertex grouping keys. Any other argument may be
   * {@code null}.
   * <p>
   * Note: To group vertices/edges by their type label, one needs to add the specific symbol
   * {@link Grouping#LABEL_SYMBOL} to the respective grouping keys.
   *
   * @param vertexGroupingKeys       property keys to group vertices
   * @param vertexAggregateFunctions aggregate functions to apply on super vertices
   * @param edgeGroupingKeys         property keys to group edges
   * @param edgeAggregateFunctions   aggregate functions to apply on super edges
   * @param groupingStrategy         execution strategy for vertex grouping
   * @return summary graph
   * @see Grouping
   */
  LogicalGraph groupBy(
    List<String> vertexGroupingKeys, List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<AggregateFunction> edgeAggregateFunctions,
    GroupingStrategy groupingStrategy);

  /**
   * Sets the aggregation result of the given function as property for each vertex. All edges where
   * the vertex is relevant get joined first and then grouped. The relevant edges are specified
   * using the direction which may direct to the vertex, or from the vertex or both.
   *
   * @param function      aggregate function
   * @param edgeDirection incoming, outgoing edges or both
   * @return logical graph where vertices store aggregated information about connected edges
   */
  LogicalGraph reduceOnEdges(
    EdgeAggregateFunction function, Neighborhood.EdgeDirection edgeDirection);

  /**
   * Sets the aggregation result of the given function as property for each vertex. All vertices
   * of relevant edges get joined first and then grouped by the vertex. The relevant edges are
   * specified using the direction which may direct to the vertex, or from the vertex or both.
   *
   * @param function      aggregate function
   * @param edgeDirection incoming, outgoing edges or both
   * @return logical graph where vertices store aggregated information about connected vertices
   */
  LogicalGraph reduceOnNeighbors(
    VertexAggregateFunction function, Neighborhood.EdgeDirection edgeDirection);

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

  /**
   * Generates all combinations of the supplied vertex grouping keys according to the definition of
   * the rollUp operation in SQL and uses them together with all edge grouping keys for separate
   * grouping operations. For example, specifying the vertex grouping keys A, B and C leads to
   * three differently grouped graphs {A,B,C},{A,B},{A} within the resulting graph collection.
   *
   * @param vertexGroupingKeys       grouping keys to group vertices
   * @param vertexAggregateFunctions aggregate functions to apply on super vertices
   * @param edgeGroupingKeys         grouping keys to group edges
   * @param edgeAggregateFunctions   aggregate functions to apply on super edges
   * @return graph collection containing all resulting graphs
   */
  GraphCollection groupVerticesByRollUp(
    List<String> vertexGroupingKeys, List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<AggregateFunction> edgeAggregateFunctions);

  /**
   * Generates all combinations of the supplied edge grouping keys according to the definition of
   * the rollUp operation in SQL and uses them together with all vertex grouping keys for separate
   * grouping operations. For example, specifying the edge grouping keys A, B and C leads to
   * three differently grouped graphs {A,B,C},{A,B},{A} within the resulting graph collection.
   *
   * @param vertexGroupingKeys       grouping keys to group vertices
   * @param vertexAggregateFunctions aggregate functions to apply on super vertices
   * @param edgeGroupingKeys         grouping keys to group edges
   * @param edgeAggregateFunctions   aggregate functions to apply on super edges
   * @return graph collection containing all resulting graphs
   */
  GraphCollection groupEdgesByRollUp(
    List<String> vertexGroupingKeys, List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<AggregateFunction> edgeAggregateFunctions);

  /**
   * Verifies this graph, removing dangling edges, i.e. edges pointing to or from
   * a vertex not contained in this graph.<br>
   * This operator can be applied after an operator that has not checked the graphs validity.
   * The graph head of this logical graph remains unchanged.
   *
   * @return this graph with all dangling edges removed.
   */
  LogicalGraph verify();

  /**
   * Verifies this graph, removing dangling graph ids from its elements,
   * i.e. ids different from this graph heads id.<br>
   * This operator can be applied after an operator that has not checked the graphs validity.
   * The graph head of this logical graph remains unchanged.
   *
   * @return this graph with all dangling graph ids removed.
   */
  LogicalGraph verifyGraphContainment();

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
  LogicalGraph callForGraph(UnaryBaseGraphToBaseGraphOperator<LogicalGraph> operator);

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
   * Creates a logical graph from that graph and other graphs using the given
   * operator.
   *
   * @param operator    multi graph to graph operator
   * @param otherGraphs other graphs
   * @return result of given operator
   */
  LogicalGraph callForGraph(GraphsToGraphOperator operator, LogicalGraph... otherGraphs);

  /**
   * Creates a graph collection from that graph using the given unary graph
   * operator.
   *
   * @param operator unary graph to collection operator
   * @return result of given operator
   */
  GraphCollection callForCollection(UnaryGraphToCollectionOperator operator);
}
