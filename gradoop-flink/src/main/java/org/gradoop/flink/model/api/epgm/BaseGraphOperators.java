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
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.api.operators.BinaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.operators.BinaryBaseGraphToValueOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphCollectionOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.operators.aggregation.Aggregation;
import org.gradoop.flink.model.impl.operators.cloning.Cloning;
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.model.impl.operators.equality.GraphEquality;
import org.gradoop.flink.model.impl.operators.exclusion.Exclusion;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.CypherPatternMatching;
import org.gradoop.flink.model.impl.operators.neighborhood.Neighborhood;
import org.gradoop.flink.model.impl.operators.neighborhood.ReduceEdgeNeighborhood;
import org.gradoop.flink.model.impl.operators.neighborhood.ReduceVertexNeighborhood;
import org.gradoop.flink.model.impl.operators.overlap.Overlap;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToIdString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToIdString;
import org.gradoop.flink.model.impl.operators.transformation.Transformation;
import org.gradoop.flink.model.impl.operators.verify.Verify;
import org.gradoop.flink.model.impl.operators.verify.VerifyGraphContainment;

import java.util.List;
import java.util.Objects;

/**
 * Defines the operators that are available on a {@link BaseGraph}.
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The type of the graph.
 * @param <GC> The type of the graph collection.
 */
public interface BaseGraphOperators<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> {

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism strategies,
   * which is vertex homomorphism and edge isomorphism. The vertex and edge data of the data graph elements
   * is attached to the resulting vertices.
   * <p>
   * Note, that this method used no statistics about the data graph which may result in bad runtime
   * performance. Use {@link #query(String, GraphStatistics)} to provide statistics for the query planner.
   *
   * @param query Cypher query
   * @return graph collection containing matching subgraphs
   */
  default GC query(String query) {
    return query(query, new GraphStatistics(1, 1, 1, 1));
  }

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism strategies,
   * which is vertex homomorphism and edge isomorphism. The vertex and edge data of the data graph elements
   * is attached to the resulting vertices.
   * <p>
   * Note, that this method used no statistics about the data graph which may result in bad runtime
   * performance. Use {@link #query(String, String, GraphStatistics)} to provide statistics for the query planner.
   * <p>
   * In addition, the operator can be supplied with a construction pattern allowing the creation of new graph
   * elements based on variable bindings of the match pattern. Consider the following example:
   * <br>
   * {@code graph.query(
   *  "MATCH (a:Author)-[:WROTE]->(:Paper)<-[:WROTE]-(b:Author) WHERE a <> b",
   *  "(a)-[:CO_AUTHOR]->(b)")}
   * <p>
   * The query pattern is looking for pairs of authors that worked on the same paper.
   * The construction pattern defines a new edge of type CO_AUTHOR between the two entities.
   *
   * @param query               Cypher query string
   * @param constructionPattern Construction pattern
   * @return graph collection containing the output of the construct pattern
   */
  default GC query(String query, String constructionPattern) {
    return query(query, constructionPattern, new GraphStatistics(1, 1, 1, 1));
  }

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism strategies,
   * which is vertex homomorphism and edge isomorphism. The vertex and edge data of the data graph elements
   * is attached to the resulting vertices.
   *
   * @param query           Cypher query
   * @param graphStatistics statistics about the data graph
   * @return graph collection containing matching subgraphs
   */
  default GC query(String query, GraphStatistics graphStatistics) {
    return query(query, true, MatchStrategy.HOMOMORPHISM, MatchStrategy.ISOMORPHISM,
      graphStatistics);
  }

  /**
   * Evaluates the given query using the Cypher query engine. The engine uses default morphism strategies,
   * which is vertex homomorphism and edge isomorphism. The vertex and edge data of the data graph elements
   * is attached to the resulting vertices.
   * <p>
   * In addition, the operator can be supplied with a construction pattern allowing the creation of new graph
   * elements based on variable bindings of the match pattern. Consider the following example:
   * <br>
   * {@code graph.query(
   *  "MATCH (a:Author)-[:WROTE]->(:Paper)<-[:WROTE]-(b:Author) WHERE a <> b",
   *  "(a)-[:CO_AUTHOR]->(b)")}
   * <p>
   * The query pattern is looking for pairs of authors that worked on the same paper.
   * The construction pattern defines a new edge of type CO_AUTHOR between the two entities.
   *
   * @param query               Cypher query
   * @param constructionPattern Construction pattern
   * @param graphStatistics     statistics about the data graph
   * @return graph collection containing the output of the construct pattern
   */
  default GC query(String query, String constructionPattern, GraphStatistics graphStatistics) {
    return query(query, constructionPattern, true, MatchStrategy.HOMOMORPHISM,
      MatchStrategy.ISOMORPHISM, graphStatistics);
  }

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
  default GC query(String query, boolean attachData, MatchStrategy vertexStrategy,
                   MatchStrategy edgeStrategy, GraphStatistics graphStatistics) {
    return query(query, null, attachData, vertexStrategy, edgeStrategy, graphStatistics);
  }

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
  default GC query(String query, String constructionPattern, boolean attachData, MatchStrategy vertexStrategy,
                   MatchStrategy edgeStrategy, GraphStatistics graphStatistics) {
    return callForCollection(new CypherPatternMatching<>(query, constructionPattern, attachData,
      vertexStrategy, edgeStrategy, graphStatistics));
  }

  /**
   * Creates a copy of the base graph.
   * <p>
   * Note that this method creates new graph head, vertex and edge instances.
   *
   * @return projected base graph
   */
  default LG copy() {
    return callForGraph(new Cloning<>());
  }

  /**
   * Creates a condensed version of the base graph by grouping vertices based on the specified
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
  default LG groupBy(List<String> vertexGroupingKeys) {
    return groupBy(vertexGroupingKeys, null);
  }

  /**
   * Creates a condensed version of the base graph by grouping vertices and edges based on given
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
  default LG groupBy(List<String> vertexGroupingKeys, List<String> edgeGroupingKeys) {
    return groupBy(vertexGroupingKeys, null, edgeGroupingKeys, null, GroupingStrategy.GROUP_REDUCE);
  }

  /**
   * Creates a condensed version of the base graph by grouping vertices and edges based on given
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
  default LG groupBy(
    List<String> vertexGroupingKeys, List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<AggregateFunction> edgeAggregateFunctions,
    GroupingStrategy groupingStrategy) {

    Objects.requireNonNull(vertexGroupingKeys, "missing vertex grouping key(s)");
    Objects.requireNonNull(groupingStrategy, "missing vertex grouping strategy");

    Grouping.GroupingBuilder builder = new Grouping.GroupingBuilder();

    builder.addVertexGroupingKeys(vertexGroupingKeys);
    builder.setStrategy(groupingStrategy);

    if (edgeGroupingKeys != null) {
      builder.addEdgeGroupingKeys(edgeGroupingKeys);
    }
    if (vertexAggregateFunctions != null) {
      vertexAggregateFunctions.forEach(builder::addVertexAggregateFunction);
    }
    if (edgeAggregateFunctions != null) {
      edgeAggregateFunctions.forEach(builder::addEdgeAggregateFunction);
    }
    return callForGraph(builder.build());
  }

  /**
   * Transforms the elements of the base graph using the given transformation functions.
   * The identity of the elements is preserved.
   *
   * @param graphHeadTransformationFunction graph head transformation function
   * @param vertexTransformationFunction    vertex transformation function
   * @param edgeTransformationFunction      edge transformation function
   * @return transformed base graph
   */
  default LG transform(
    TransformationFunction<G> graphHeadTransformationFunction,
    TransformationFunction<V> vertexTransformationFunction,
    TransformationFunction<E> edgeTransformationFunction) {
    return callForGraph(new Transformation<>(
      graphHeadTransformationFunction,
      vertexTransformationFunction,
      edgeTransformationFunction));
  }

  /**
   * Transforms the graph head of the base graph using the given
   * transformation function. The identity of the graph is preserved.
   *
   * @param graphHeadTransformationFunction graph head transformation function
   * @return transformed base graph
   */
  default LG transformGraphHead(TransformationFunction<G> graphHeadTransformationFunction) {
    return transform(graphHeadTransformationFunction, null, null);
  }

  /**
   * Transforms the vertices of the base graph using the given transformation
   * function. The identity of the vertices is preserved.
   *
   * @param vertexTransformationFunction vertex transformation function
   * @return transformed base graph
   */
  default LG transformVertices(TransformationFunction<V> vertexTransformationFunction) {
    return transform(null, vertexTransformationFunction, null);
  }

  /**
   * Transforms the edges of the base graph using the given transformation function.
   * The identity of the edges is preserved.
   *
   * @param edgeTransformationFunction edge transformation function
   * @return transformed base graph
   */
  default LG transformEdges(TransformationFunction<E> edgeTransformationFunction) {
    return transform(null, null, edgeTransformationFunction);
  }

  /**
   * Returns the subgraph that is induced by the vertices which fulfill the given filter function.
   *
   * @param vertexFilterFunction vertex filter function
   * @return vertex-induced subgraph
   */
  default LG vertexInducedSubgraph(FilterFunction<V> vertexFilterFunction) {
    Objects.requireNonNull(vertexFilterFunction);
    return callForGraph(
      new Subgraph<>(vertexFilterFunction, null, Subgraph.Strategy.VERTEX_INDUCED));
  }

  /**
   * Returns the subgraph that is induced by the edges which fulfill the given filter function.
   *
   * @param edgeFilterFunction edge filter function
   * @return edge-induced subgraph
   */
  default LG edgeInducedSubgraph(FilterFunction<E> edgeFilterFunction) {
    Objects.requireNonNull(edgeFilterFunction);
    return callForGraph(new Subgraph<>(null, edgeFilterFunction, Subgraph.Strategy.EDGE_INDUCED));
  }

  /**
   * Returns a subgraph of the base graph which contains only those vertices
   * and edges that fulfil the given vertex and edge filter functions respectively.
   * <p>
   * Note, that the operator does not verify the consistency of the resulting graph.
   * Use {@link #verify()} for that behaviour.
   *
   * @param vertexFilterFunction vertex filter function
   * @param edgeFilterFunction   edge filter function
   * @return base graph which fulfils the given predicates and is a subgraph of that graph
   */
  default LG subgraph(FilterFunction<V> vertexFilterFunction,
                      FilterFunction<E> edgeFilterFunction) {
    Objects.requireNonNull(vertexFilterFunction);
    Objects.requireNonNull(edgeFilterFunction);
    return subgraph(vertexFilterFunction, edgeFilterFunction, Subgraph.Strategy.BOTH);
  }

  /**
   * Returns a subgraph of the base graph which contains only those vertices
   * and edges that fulfil the given vertex and edge filter functions respectively.
   * <p>
   * Note, that the operator does not verify the consistency of the resulting graph.
   * Use {@link #verify()} for that behaviour.
   *
   * @param vertexFilterFunction vertex filter function
   * @param edgeFilterFunction   edge filter function
   * @param strategy             execution strategy for the operator
   * @return base graph which fulfils the given predicates and is a subgraph of that graph
   */
  default LG subgraph(FilterFunction<V> vertexFilterFunction,
                      FilterFunction<E> edgeFilterFunction, Subgraph.Strategy strategy) {
    return callForGraph(new Subgraph<>(vertexFilterFunction, edgeFilterFunction, strategy));
  }

  /**
   * Applies the given aggregate functions to the base graph and stores the
   * result of those functions at the resulting graph using the given property keys.
   *
   * @param aggregateFunctions computes aggregates on the base graph
   * @return base graph with additional properties storing the aggregates
   */
  default LG aggregate(AggregateFunction... aggregateFunctions) {
    return callForGraph(new Aggregation<>(aggregateFunctions));
  }

  /**
   * Sets the aggregation result of the given function as property for each vertex. All edges where
   * the vertex is relevant get joined first and then grouped. The relevant edges are specified
   * using the direction which may direct to the vertex, or from the vertex or both.
   *
   * @param function      aggregate function
   * @param edgeDirection incoming, outgoing edges or both
   * @return base graph where vertices store aggregated information about connected edges
   */
  default LG reduceOnEdges(EdgeAggregateFunction function,
                           Neighborhood.EdgeDirection edgeDirection) {
    return callForGraph(new ReduceEdgeNeighborhood<>(function, edgeDirection));
  }

  /**
   * Sets the aggregation result of the given function as property for each vertex. All vertices
   * of relevant edges get joined first and then grouped by the vertex. The relevant edges are
   * specified using the direction which may direct to the vertex, or from the vertex or both.
   *
   * @param function      aggregate function
   * @param edgeDirection incoming, outgoing edges or both
   * @return base graph where vertices store aggregated information about connected vertices
   */
  default LG reduceOnNeighbors(VertexAggregateFunction function,
                               Neighborhood.EdgeDirection edgeDirection) {
    return callForGraph(new ReduceVertexNeighborhood<>(function, edgeDirection));
  }

  /**
   * Verifies this graph, removing dangling edges, i.e. edges pointing to or from
   * a vertex not contained in this graph.<br>
   * This operator can be applied after an operator that has not checked the graphs validity.
   * The graph head of this base graph remains unchanged.
   *
   * @return this graph with all dangling edges removed.
   */
  default LG verify() {
    return callForGraph(new Verify<>());
  }

  /**
   * Verifies this graph, removing dangling graph ids from its elements,
   * i.e. ids different from this graph heads id.<br>
   * This operator can be applied after an operator that has not checked the graphs validity.
   * The graph head of this base graph remains unchanged.
   *
   * @return this graph with all dangling graph ids removed.
   */
  default LG verifyGraphContainment() {
    return callForGraph(new VerifyGraphContainment<>());
  }

  //----------------------------------------------------------------------------
  // Binary Operators
  //----------------------------------------------------------------------------

  /**
   * Creates a new base graph by combining the vertex and edge sets of
   * this graph and the given graph. Vertex and edge equality is based on their identifiers.
   *
   * @param otherGraph base graph to combine this graph with
   * @return base graph containing all vertices and edges of the input graphs
   */
  default LG combine(LG otherGraph) {
    return callForGraph(new Combination<>(), otherGraph);
  }

  /**
   * Creates a new base graph containing the overlapping vertex and edge sets of this graph
   * and the given graph. Vertex and edge equality is based on their identifiers.
   *
   * @param otherGraph base graph to compute overlap with
   * @return base graph that contains all vertices and edges that exist in both input graphs
   */
  default LG overlap(LG otherGraph) {
    return callForGraph(new Overlap<>(), otherGraph);
  }

  /**
   * Creates a new base graph containing only vertices and edges that exist in that graph
   * but not in the other graph. Vertex and edge equality is based on their identifiers.
   * The graph head of this graph is retained.
   *
   * @param otherGraph base graph to exclude from that graph
   * @return base that contains only vertices and edges that are not in the other graph
   */
  default LG exclude(LG otherGraph) {
    return callForGraph(new Exclusion<>(), otherGraph);
  }

  /**
   * Checks, if another graph contains exactly the same vertices and edges (by id) as this graph.
   *
   * @param other other graph
   * @return 1-element dataset containing true, iff equal by element ids
   */
  default DataSet<Boolean> equalsByElementIds(LG other) {
    return callForValue(new GraphEquality<>(
      new GraphHeadToEmptyString<>(),
      new VertexToIdString<>(),
      new EdgeToIdString<>(), true), other);
  }

  /**
   * Checks, if another graph contains vertices and edges with the same
   * attached data (i.e. label and properties) as this graph.
   *
   * @param other other graph
   * @return 1-element dataset containing true, iff equal by element data
   */
  default DataSet<Boolean> equalsByElementData(LG other) {
    return callForValue(new GraphEquality<>(
      new GraphHeadToEmptyString<>(),
      new VertexToDataString<>(),
      new EdgeToDataString<>(), true), other);
  }

  /**
   * Checks, if another graph has the same attached data and contains
   * vertices and edges with the same attached data as this graph.
   *
   * @param other other graph
   * @return 1-element dataset containing true, iff equal by element data
   */
  default DataSet<Boolean> equalsByData(LG other) {
    return callForValue(new GraphEquality<>(
      new GraphHeadToDataString<>(),
      new VertexToDataString<>(),
      new EdgeToDataString<>(), true), other);
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  /**
   * Returns a 1-element dataset containing a {@code boolean} value which indicates if the graph is empty.
   *
   * A graph is considered empty, if it contains no vertices.
   *
   * @return  1-element dataset containing {@code true}, if the collection is
   *          empty or {@code false} if not
   */
  DataSet<Boolean> isEmpty();

  //----------------------------------------------------------------------------
  // Call for Operators
  //----------------------------------------------------------------------------

  /**
   * Creates a value using the given unary graph to value operator.
   *
   * @param operator unary graph to value operator
   * @param <T> return type
   * @return result of given operator
   */
  <T> T callForValue(UnaryBaseGraphToValueOperator<LG, T> operator);

  /**
   * Calls the given binary graph to value operator using this graph and the input graph.
   *
   * @param operator   binary graph to value operator
   * @param otherGraph second input graph for operator
   * @param <T> return type
   * @return result of given operator
   */
  <T> T callForValue(BinaryBaseGraphToValueOperator<LG, T> operator, LG otherGraph);

  /**
   * Creates a base graph using the given unary graph operator.
   *
   * @param operator unary graph to graph operator
   * @return result of given operator
   */
  default LG callForGraph(UnaryBaseGraphToBaseGraphOperator<LG> operator) {
    return callForValue(operator);
  }

  /**
   * Creates a base graph from this graph and the input graph using the given binary operator.
   *
   * @param operator   binary graph to graph operator
   * @param otherGraph other graph
   * @return result of given operator
   */
  default LG callForGraph(BinaryBaseGraphToBaseGraphOperator<LG> operator, LG otherGraph) {
    return callForValue(operator, otherGraph);
  }
  /**
   * Creates a graph collection from this graph using the given unary graph operator.
   *
   * @param operator unary graph to collection operator
   * @return result of given operator
   */
  default GC callForCollection(UnaryBaseGraphToBaseGraphCollectionOperator<LG, GC> operator) {
    return callForValue(operator);
  }
}
